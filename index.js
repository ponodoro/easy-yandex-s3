const AWS = require('aws-sdk');
const md5 = require('md5');
const fs = require('fs');
const path = require('path');
const fileType = require('file-type');
const mime = require('mime-types');
const uniqid = require('uniqid');
const { IoTFleetHub } = require('aws-sdk');

/**
 * Создание объекта для работы с S3 хранилищем
 */
class EasyYandexS3 {
  /**
   * 
  * @param {Object} params Параметры соединения, 4 обязательных параметра.
  * @param {Object} params.auth Обязательно. Данные для доступа от сервисного аккаунта
  * @param {String} params.auth.accessKeyId Обязательно. Идентификатор ключа сервисного аккаунта
  * @param {String} params.auth.secretAccessKey Обязательно. Cекретный ключ сервисного аккаунта
  * @param {String} params.Bucket Обязательно. ID бакета
  * 
  * @param {String=} params.endpoint_url Необязательно. Ссылка на S3 сервер, например, на storage.yandexcloud.net
  * @param {String=} params.region Необязательно. Регион загрузки
  * @param {Object=} params.httpOptions Необязательно. Установки http-запроса
  * @param {Boolean=} params.debug Необязательно. Вывод дополнительной информации в консоль
   */
  constructor(params) {
    this.default_params = {
      endpoint_url: 'https://storage.yandexcloud.net',
      auth: {
        accessKeyId: '',
        secretAccessKey: ''
      },
      region: 'us-east-1',
      httpOptions: {
        timeout: 10000,
        connectTimeout: 10000
      },
      Bucket: '',
      debug: false
    };
    const new_params = { ...this.default_params, ...params };
    this.threads = {};

    this.s3 = new AWS.S3({
      endpoint: new AWS.Endpoint(new_params.endpoint_url),
      accessKeyId: new_params.auth.accessKeyId,
      secretAccessKey: new_params.auth.secretAccessKey,
      region: new_params.region,
      httpOptions: new_params.httpOptions
    });

    this.debug = new_params.debug;
    this.Bucket = params.Bucket;

    this.default_ignore_list = ['.DS_Store'];
  };

  _debugLog() {
    if (debug) {
      this._log(...arguments);
    };
  };

  _log() {
    return console.log('[' + (new Date().toUTCString()) + '] ', ...arguments)
  };


  /**
   * Загрузка файла
   * @param {Object|Array<Object>} file Буфер файла и информация о расширении. Или путь к файлу.
   * @param {Buffer=} file.buffer Буфер файла
   * @param {String=} file.path Путь к файлу
   * @param {Boolean=} file.save_name Оставить оригинальное название файла. Работает только в случае передачи пути к файлу.
   * @param {String=} file.name Устаналивает название загружаемому файлу. Передавать с расширением.
   * @param {Array=} file.ignore Список игнорируемых файлов и папок
   * 
   * 
   * @param {String} route Папка загрузки - бакет
   * 
   * @returns {Promise<Object>} Результат загрузки
   */
  async Upload(file, route) {
    let fileBody;
    let fileExt;
    let fileMD5;
    let fileUploadName;

    const debug = this.debug;
    const debug_object = 'upload';

    if (Array.isArray(file)) {
      if (file.length == 0) {
        throw 'file array is empty';
      };

      this._debugLog('S3', debug_object, 'array to upload:', file.length);

      const uploadResult = await this._uploadArray(file, route);
      this._debugLog('S3', debug_object, 'array upload done', uploadResult.length, 'files');

      return uploadResult;
    }

    if (file.path) {
      const path = path.join(file.path);

      if (!fs.existsSync(path)) { 
        throw `file/directory on path is not found (${path})`; 
      };
      if (!route) { 
        throw 'route (2nd argument) is not defined'; 
      };

      if (fs.lstatSync(path).isDirectory()) {
        this._debugLog('S3', debug_object, 'folder to upload found', path);
        if (!file.ignore) {
          file.ignore = [];
        };

        const ignoreList = [...this.default_ignore_list, ...file.ignore]
        const uploadResult = await this._uploadDirectory(path, file, route, ignoreList);

        this._debugLog('S3', debug_object, 'folder upload done', u.length, 'files');
        return uploadResult;
      }
      
      fileBody = fs.readFileSync(file.path);
      fileExt = path.extname(file.path);
      if (file.saveName) {
        fileUploadName = path.basename(file.path);
      };
      if (file.name) {
        fileUploadName = file.name;
      };
    } else {
      fileBody = file.buffer;
      try {
        fileExt = `.${fileExt(file)}`;
      } catch (error) {
        this._debugLog('S3', debug_object, 'error:', error.message);
      };
      if (file.name) {
        fileUploadName = file.name;
      };
    };

    if (route.slice(-1) != '/') {
      route += '/';
    };
    if (route[0] == '/') {
      route = route.slice(1);
    };

    fileMD5 = md5(file_body);
    if (!fileUploadName) {
      fileUploadName = `${fileMD5}${fileExt}`;
    };

    let Key = `${route}${fileUploadName}`;
    let Body = fileBody;

    let Bucket = this.Bucket;
    let ContentType = mime.lookup(fileUploadName) || 'text/plain';
    const params = { 
      Bucket, 
      Key, 
      Body, 
      ContentType,
    };

    this._debugLog('S3', debug_object, 'started');
    this._debugLog('S3', debug_object, params);

    try {
      const uploadResult = await this.s3.upload(params).promise();
      this._debugLog('S3', debug_object, 'done:', s3Promise);

      return s3Promise;
    } catch (error) {
      this._debugLog('S3', debug_object, 'error:', error.message);

      return false;
    }
  };


  async _uploadDirectory(dir, params, route, ignore_list) {
    let debug = this.debug;
    let debug_object = '_uploadDirectory';

    let uploaded = [];
    let threads = sliceArrayToThreads(readDir(dir, dir, ignore_list), 1); // TODO params.threads

    // TODO Threads
    // let thread_id = uniqid();
    // let new_threads = this.threads[thread_id];
    // this.threads[thread_id] = {proccesses: [], results: [], callback: params.callback};

    if (debug) this._log('S3', debug_object, 'threads divided:', threads.length);

    for (let thread_files of threads) {
      for (let file of thread_files) {
        params.path = file.full_file_path;
        if (params.name) params.name = false;
        if (params.save_name) params.name = file.file_name;
        if (debug) this._log('S3', debug_object, 'dir file uploading:', route, 'to', file.relative_dir_path);

        // TODO Threads
        // new_threads.proccesses.push(
        // 	new Promise(async function(resolve, reject, new_threads) {
        // 		let u = await this.Upload(params, path.resolve(route, file.relative_dir_path));
        // 		new_threads.results.push(u);
        // 		if(new_threads.proccesses.length == new_threads.results) new_threads.callback(new_threads.results);
        // 	})
        // );
        let u = await this.Upload(params, path.join(route, file.relative_dir_path));
        uploaded.push(u);
      }
    }
    return uploaded;
  }

  async _uploadArray(array, route) {
    let debug = this.debug;
    let debug_object = '_uploadArray';

    let uploaded = [];

    for (let file of array) {
      let u = await this.Upload(file, route);
      uploaded.push(u);
    }
    return uploaded;
  }


  /**
   * Получение списка директорий и папок
   * @param {String=} route Необязательно. Путь к папке, которую смотрим
   * 
   * @returns {Promise<Object>} Результат просмотра
  */
  async GetList(route) {
    if (!route) route = '/';
    if (route == './') route = '/';
    if (route) route += route.slice(-1) != '/' ? '/' : '';
    if (route[0] == '.') route = route.slice(1);
    if (route[0] == '/') route = route.slice(1);

    let s3 = this.s3;
    let Bucket = this.Bucket;
    let params = {
      Bucket,
      Prefix: route,
      Delimiter: '/'
    }

    let debug = this.debug;
    let debug_object = 'listObjectsV2'
    if (debug) this._log('S3', debug_object, 'started');
    if (debug) this._log('S3', debug_object, params);

    try {
      let s3Promise = await new Promise(function (resolve, reject) {
        s3.listObjectsV2(params, function (err, data) {
          if (err) return reject(err);
          return resolve(data);
        });
      });
      if (debug) this._log('S3', debug_object, 'done:', s3Promise);
      return s3Promise;
    } catch (error) {
      if (debug) this._log('S3', debug_object, 'error:', error);
      return false;
    }
  }


  /**
   * Скачивание файла
   * @param {String} route_full_path Полный путь до файла. С папками, с названием и расширением файла
   * @param {String=} destination_full_path Необязательно. Куда сохраняем файл. Абсолютный или относительный, с названием и расширением файла
   * 
   * @returns {Promise<Object>} Результат скачивания и сохранения
  */
  async Download(route_full_path, destination_full_path) {
    if (route_full_path[0] == '/') route_full_path = route_full_path.slice(1);
    if (!destination_full_path) destination_full_path = false;

    let s3 = this.s3;
    let Bucket = this.Bucket;
    let Key = route_full_path;
    let params = {
      Bucket,
      Key
    }

    let debug = this.debug;
    let debug_object = 'getObject'
    if (debug) this._log('S3', debug_object, 'started');
    if (debug) this._log('S3', debug_object, params);

    try {
      let s3Promise = await new Promise(function (resolve, reject) {
        s3.getObject(params, function (err, data) {
          if (err) return reject(err);
          // data

          let file_replaced = false;
          if (destination_full_path) {
            let buffer = data.Body;
            if (fs.existsSync(destination_full_path)) file_replaced = true;
            fs.writeFileSync(destination_full_path, buffer);
          }

          return resolve({
            data,
            destination_full_path,
            file_replaced
          });
        });
      });
      if (debug) this._log('S3', debug_object, 'done:', s3Promise);
      return s3Promise;
    } catch (error) {
      if (debug) this._log('S3', debug_object, 'error:', error);
      return false;
    }
  }


  /**
   * Удаление файла из хранилища
   * @param {String} route_full_path Полный путь до файла. С папками, с названием и расширением файла
   * 
   * @returns {Promise<Object>} Результат удаления
  */
  async Remove(route_full_path) {
    if (route_full_path[0] == '/') route_full_path = route_full_path.slice(1);

    let s3 = this.s3;
    let Bucket = this.Bucket;
    let Key = route_full_path;
    let params = { Bucket, Key };

    let debug = this.debug;
    let debug_object = 'deleteObject'
    if (debug) this._log('S3', debug_object, 'started');
    if (debug) this._log('S3', debug_object, params);

    try {
      let s3Promise = await new Promise(function (resolve, reject) {
        s3.deleteObject(params, function (err, data) {
          if (err) return reject(err);
          return resolve(true);
        });
      });
      if (debug) this._log('S3', debug_object, 'done:', s3Promise);
      return s3Promise;
    } catch (error) {
      if (debug) this._log('S3', debug_object, 'error:', error);
      return false;
    }
  }
}


/**
 * Получение массива всех вложенных файлов и папок и их файлов и папок и их файлов и папок...
 * @param {String} dir_path Путь до папки, которую сканируем
 * @param {String=} original_file_path 
 * @param {Array} ignore_list
 */
function readDir(dir_path, original_file_path, ignore_list) {
  if (!original_file_path) original_file_path = dir_path;
  let dir_files = fs.readdirSync(dir_path);
  let paths = [];
  for (let file_name of dir_files) {
    let full_file_path = path.join(dir_path, file_name);
    let relative_file_path = full_file_path.replace(original_file_path, '');
    let relative_dir_path = relative_file_path.replace(file_name, '');

    if (ignore_list.includes(file_name)) continue;
    if (ignore_list.includes(relative_file_path.replace(/\\/g, '/')) || ignore_list.includes(relative_file_path.replace(/\//g, '\\'))) continue;

    if (fs.lstatSync(full_file_path).isDirectory()) {
      paths.push(...readDir(full_file_path, original_file_path, ignore_list));
      continue;
    }
    paths.push({ full_file_path, relative_dir_path, file_name });
  }
  // console.log(paths);
  return paths;
}

/**
 * Расширенное определение расширения файла
 * https://github.com/powerdot/easy-yandex-s3/commit/8e5f3e42a5dffe6e54ceef16288e5a9c00868838
 * @param {*} file 
 */
function fileExt(file) {
  if (file.mimetype) {
    switch (file.mimetype) {
      case 'text/plain': return 'txt';
      case 'application/msword': return 'doc';
      case 'application/vnd.ms-excel': return 'xls';
      case 'text/csv': return 'csv';
      default: return fileType(file.buffer).ext;
    };
  } else {
    if (file.buffer) return fileType(file.buffer).ext;
  }

  return '';
};


/**
 * Разделение массива на n- массивов. В зависимости от аргумента threads (n). Спасибо, Stackoverflow.
 * https://stackoverflow.com/questions/41964628/slice-array-into-an-array-of-arrays
 * @param {Array} arr Массив
 * @param {Number} threads Потоки
 */
function sliceArrayToThreads(arr, threads) {
  if (!threads) threads = 1;
  if (threads <= 0) threads = 1;
  let size = arr.length / threads;
  let step = 0, sliceArr = [], len = arr.length;
  while (step < len) {
    sliceArr.push(arr.slice(step, step += size));
  }
  return sliceArr;
}

module.exports = EasyYandexS3;
