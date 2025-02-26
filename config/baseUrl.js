exports.domainURL = async (baseUrl) => {
  return new Promise(async (resolve, reject) => {
    let url = '';
  
    if (baseUrl == 'http://localhost:3212/') {
      url = 'http://localhost:3000/'
    }
    else if (baseUrl == 'https://dev-server.fob.sofodev.co/') {
      url = 'https://dev.fob.sofodev.co/'
    }
    else if (baseUrl == 'http://dev.fob.sofodev.co/') {
      url = 'https://dev.fob.sofodev.co/'
    }
    else if (baseUrl == 'https://dev.fob.sofodev.co/') {
      url = 'https://dev.fob.sofodev.co/'
    }
    else if (baseUrl == 'http://dev-server.fob.sofodev.co/') {
      url = 'https://dev.fob.sofodev.co/'
    }
    else if (baseUrl == 'https://server.tradereboot.com/') {
      url = 'https://app.tradereboot.com/'
    }
    else if (baseUrl == 'https://app.tradereboot.com/') {
      url = 'https://app.tradereboot.com/'
    }
    else if (baseUrl == 'http://server.tradereboot.com/') {
      url = 'https://app.tradereboot.com/'
    }
  
    resolve(url);
  })
}