const BaseCacher = require('moleculer').Cachers.Base
const { METRIC } = require('moleculer')
const fs = require('fs')
const path = require('path')

class FsCacher extends BaseCacher {

  /**
   * Creates an instance of FsCacher.
   *
   * @param {object} opts
   *
   * @memberof FsCacher
   */
  constructor(opts) {
    super(opts)
    this.prefixes = opts.prefixes
    this.defaultPrefix = opts.defaultPrefix
    this.fsCacherPath = opts.fsCacherPath
    this.sublevels = opts.sublevels || 0
  }

  /**
   * Initialize cacher
   *
   * @param {any} broker
   *
   * @memberof FsCacher
   */
  init (broker) {
    this.prefixes.forEach(prefix => {
      if (!fs.existsSync(path.join(this.fsCacherPath, prefix))) {
        fs.mkdirSync(path.join(this.fsCacherPath, prefix))
      }
    })
    return super.init(broker)
  }

  /**
   * Get data from cache by key
   *
   * @param {any} key
   * @returns {Promise}
   *
   * @memberof FsCacher
   */
  async get (key, fsCacherPath) {
    this.logger.debug(`GET ${key}`)
    this.metrics.increment(METRIC.MOLECULER_CACHER_GET_TOTAL)
    const timeEnd = this.metrics.timer(METRIC.MOLECULER_CACHER_GET_TIME)
    const fullPath = await this._getFullPath(key, fsCacherPath)
    const stats = await this._fileExists(fullPath)
    if (stats) {
      this.logger.debug(`FOUND ${key}`)
      this.metrics.increment(METRIC.MOLECULER_CACHER_FOUND_TOTAL)
      let item = await this._readFile(fullPath)
      try {
        item = JSON.parse(item)
      } catch (ex) {
        this.logger.debug(`item is not a valid JSON, returning string. ${key}`)
      }
      timeEnd()
      return item
    } else {
      timeEnd()
    }
    return null
  }

  /**
   * Save data to cache by key
   *
   * @param {String} key
   * @param {any} data JSON object
   * @returns {Promise}
   *
   * @memberof FsCacher
   */
  async set (key, data, fsCacherPath, stringifier = JSON.stringify) {
    if (data) {
      this.metrics.increment(METRIC.MOLECULER_CACHER_SET_TOTAL)
      const timeEnd = this.metrics.timer(METRIC.MOLECULER_CACHER_SET_TIME)
      const fullPath = await this._getFullPath(key, fsCacherPath)
      await this._writeFile(fullPath, typeof data === 'string' ? data : stringifier(data))
      timeEnd()
      this.logger.debug(`SET ${key}`)
    }
    return path.relative(this.fsCacherPath, fullPath)
  }

  /**
   * Delete a key from cache
   *
   * @param {string|Array<string>} key
   * @returns {Promise}
   *
   * @memberof FsCacher
   */
  async del (keys) {
    this.metrics.increment(METRIC.MOLECULER_CACHER_DEL_TOTAL)
    const timeEnd = this.metrics.timer(METRIC.MOLECULER_CACHER_DEL_TIME)
    keys = Array.isArray(keys) ? keys : [keys]
    for (const key of keys) {
      const fullPath = await this._getFullPath(key)
      await fs.promises.unlink(fullPath)
      this.logger.debug(`REMOVE ${key}`)
    }
    timeEnd()
    return null
  }

  async _getFullPath (key, fsCacherPath) {
    const sublevels = fsCacherPath ? 0 : this.sublevels
    const keyParts = key.split(':').filter(el => el !== this.defaultPrefix)
    const id = keyParts.pop()
    const idParts = []
    for (let i = 0; i < sublevels; ++i) {
      idParts.push(id[i])
    }
    const fileName = id.substr(sublevels, id.length - sublevels)
    const folderPath = path.join(fsCacherPath || this.fsCacherPath, ...keyParts, ...idParts)
    if (!(await this._fileExists(folderPath))) {
      await fs.promises.mkdir(folderPath, { recursive: true })
    }
    return path.join(folderPath, fileName)
  }

  _fileExists (file) { // returns stats object
    return fs.promises.stat(file, fs.constants.F_OK).then(stat => stat).catch(() => false)
  }

  _readFile (file) {
    return fs.promises.readFile(file, { encoding: 'utf8' })
  }

  _writeFile (file, data) {
    return fs.promises.writeFile(file, data, { encoding: 'utf8' })
  }

  async _throughDirectoryWrapper (dir) {
    const files = []
    await this._throughDirectory(dir, files)
    return files
  }

  async _throughDirectory (dir, files) {
    const _files = await fs.promises.readdir(dir)
    for (const file of _files) {
      const abs = path.join(dir, file)
      if ((await fs.promises.stat(abs)).isDirectory()) return this._throughDirectory(abs, files)
      else return files.push(abs)
    }
  }
}

module.exports = FsCacher