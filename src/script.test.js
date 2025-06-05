alert('test script to be excluded')
const http = require('http');
const url = require('url');

http.createServer(function (req, res) {
    const query = url.parse(req.url, true).query;
    // Vulnerable to reflected XSS
    res.end('<h1>Hello, ' + query.name + '!</h1>');
}).listen(8080);
