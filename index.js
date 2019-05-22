var Readable = require('stream').Readable;
var util = require('util');
var debug = require('debug')('s3-download-stream');
var SimpleQueue = require('SimpleQueue');
var clone = require('clone');
util.inherits(S3Readable, Readable);

module.exports = S3Readable;

function S3Readable(opts) {
  opts = opts || {};
  if (!(this instanceof S3Readable)) return new S3Readable(opts);
  if (!opts.getTempCredentials) throw Error("S3Readable: getTempCredentials option required (function that returns a promise with the temporal credentials from cognito)")
  if (!opts.params) throw Error("S3Readable: params option required for getObject call")
  if (!opts.params.Key) throw Error("S3Readable: Key option required for getObject call")
  Readable.call(this, opts)

  this.client = null;
  this.params = opts.params;
  this.getTempCredentials = opts.getTempCredentials;

  this.bytesReading = 0;
  this.working = 0;
  this.done = false;
  // allow for setting chunkSize = null so that default read amount kicks in
  // otherwise, use an optimized chunksize for downloading
  this.chunkSize = opts.chunkSize === undefined ? 1024 * 512 : opts.chunkSize;
  this.concurrency = opts.concurrency || 6;
  this.queue = new SimpleQueue(worker, processed, null, this.concurrency)
  var self = this;

  function worker(numBytes, done) {
    self.working += 1
    self.sip(self.bytesReading, numBytes, done);
    self.bytesReading += numBytes;
  }

  function processed(err, result) {
    if (self.done) return;
    if (err) {
      self.done = true;
      if (err.code == 'NoSuchKey')
        err.notFound = true

      return self.emit('error', err)
    }
    self.working -= 1
    debug("%s working %d queue %d %s", self.params.Key, self.working, self.queue._queue.length, result.range);
    self.done = result.data === null;
    self.push(result.data);
  }
}

S3Readable.prototype._read = function (numBytes) {
  var toQueue = this.concurrency - this.queue._queue.length;
  for (var i = 0; i < toQueue; ++i)
    this.queue.push(this.chunkSize || numBytes);
}

S3Readable.prototype.sip = function (from, numBytes, done) {
  var self = this;
  var params = clone(this.params)
  var rng = params.Range = range(from, numBytes)

  const downloadChunk = () =>
    self.client.getObject(params, function (err, res) {
      // range is past EOF, can return safely
      if (err && err.statusCode === 416) return done(null, { data: null })
      if (err) return done(err)

      var contentLength = +res.ContentLength;
      var data = contentLength === 0 ? null : res.Body;
      done(null, { range: rng, data: data, contentLength: contentLength });
    });

  if (!self.client || !self.client.credentials || self.client.credentials.needsRefresh()) {
    debug("AWS temp credentials expired or inexisting. Getting new ones...");
    self.getTempCredentials().then(({ credentials, awsS3Bucket }) => {
      debug("AWS temp credentials retrieved. Now creating a new S3 client...");
      self.client = new AWS.S3({ params: { ...self.params, Bucket: awsS3Bucket }, credentials });
      debug("New S3 Client created. Now continuing downloading chunks...");
      downloadChunk()
    })
  } else
    downloadChunk()
}

function range(from, toRead) {
  return util.format("bytes=%d-%d", from, from + toRead - 1);
}
