const path = require('path');
const fs = require('fs');

module.exports = {
  process(src, filename) {
    const json = JSON.stringify(fs.readFileSync(filename, 'utf8'));
    return {
      code: `module.exports = ${json};`,
    };
  },
}; 