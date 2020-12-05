// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

/**
 * @constructor
 */
var PopupController = function () {
  this.button_ = document.getElementById('button');
  this.api_ = document.getElementById('api');
  this.maxPage_ = document.getElementById('maxPage');
  //this.result_ = document.getElementById('result');
  this.addListeners_();
};

PopupController.prototype = {

  button_: null,

  api_: null,

  maxPage: 200,



  addListeners_: function () {
    this.button_.addEventListener('click', this.handleClick_.bind(this));
  },


  sendRequest: function(type, url, data, async=true, callback){

    var xhr = new XMLHttpRequest();
    xhr.open(type, url, async);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.onreadystatechange = function () {
        if (xhr.readyState == 4) {
            // defensive check
            if (typeof callback === "function") {
                // apply() sets the meaning of "this" in the callback
                callback.apply(xhr);
            }
        }
    };

    xhr.send(data);

  },

  handleClick_: function () {
    /*setTimeout(() => {
      chrome.tabs.query({ currentWindow: true, active: true }, function (tabs) {
        chrome.tabs.update(tabs[0].id, {url:'https://www.aliexpress.com'})
      });
    }, 1000);*/

    _this = this
    _this.button_.setAttribute('disabled', 'disabled');
    _this.button_.innerText = 'Crawling...';
    //_this.label_.style.visibilty = 'visible';
    //_this.initiated_ = true;

    var message = document.querySelector('#message');

    _this.sendRequest("GET",  _this.api_.value.split('--')[0], null, true, function(){
      alert(_this.api_.value.split('--')[0], _this.api_.value.split('--')[1])
       chrome.runtime.getBackgroundPage((background) => background.setMaxPage(_this.maxPage_.value));
       chrome.runtime.getBackgroundPage((background) => background.setURL( _this.api_.value.split('--')[1]));
      //chrome.runtime.getBackgroundPage((background) => background.resetVars());
      chrome.runtime.getBackgroundPage((background) => background.setKeywords(JSON.parse(this.responseText)));
      //window.close();
      chrome.tabs.executeScript(null, {
        file: "getPagesSource.js"
      }, function() {
        
        // If you try and inject into an extensions page or the webstore/NTP you'll get an error
        if (chrome.runtime.lastError) {
          alert(chrome.runtime.lastError.message)
        }
      });


    });
  }
};

document.addEventListener('DOMContentLoaded', function () {
  window.PC = new PopupController();
});


