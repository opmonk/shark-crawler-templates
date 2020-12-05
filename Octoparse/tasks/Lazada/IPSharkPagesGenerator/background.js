var page = 1;
var pages = null
var initiated = false;
var resultCount = 0
var resultPerPage = 60
var keywords = [];
var maxPage = 60 //60 orig
var currentKey = 0 //change to 0
var id = null
var rows = []
var url = ''

function initiate(){
    initiated = true
}

function resetVars(){
   page = 1;
   pages = null
   initiated = false;
   resultCount = 0
   resultPerPage = 60
   maxPage = 10 //60 orig
   currentKey = 0 // change to 0
   id = null
}

function setKeywords(kwords){

  keywords = kwords

}

function setURL(u){
  url = u

}


function setMaxPage(maxPage){
    maxPage = maxPage;
}

function updateTab(id, url){
  chrome.tabs.update(id, {url: url});
}


function nextKeyword(tabId){
    currentKey++;
    if(  currentKey < keywords.length && keywords[currentKey] != 'undefined'){
      updateTab(tabId, url+(keywords[currentKey].keyword).replace(/\\/g, ''))
    }else{
        let content = "" //(browser.name == "safari") ? "" : "data:text/csv;charset=utf-8,";
        rows.forEach(function(row){
            content += row + "\r\n";
            
        }); 

        //if(browser.name == "safari"){
        var blob = new Blob(["\ufeff", content]);
        var a = window.document.createElement("a");
        a.href = window.URL.createObjectURL(blob, {type: "text/plain"});
        a.download = "generated urls.txt";
        document.body.appendChild(a);
            a.click();
        document.body.removeChild(a);
    }
}



chrome.runtime.onMessage.addListener(function(request, sender) {
  if (request.action == "getSource") {
    var str = request.source;

     var regexCaptcha = /"deny-h5-tips"/gm;
    var c, doCaptcha = false;
    while ((c = regexCaptcha.exec(str)) !== null) {
        // This is necessary to avoid infinite loops with zero-width matches
        if (c.index === regexCaptcha.lastIndex) {
            regexCaptcha.lastIndex++;
        }
       
        // The result can be accessed through the `c`-variable.
        c.forEach((match, groupIndex) => {
            if(groupIndex === 0){
                doCaptcha = true
            }
        });
    }


    if ( doCaptcha ){
      initiated = false;
    }else{

      if(!initiated){
          initiated = true
          chrome.tabs.query({ currentWindow: true, active: true }, function (tabs) {
            id = Date.now();
            updateTab(tabs[0].id, url+(keywords[currentKey].keyword).replace(/\\/g, ''))
          });
          return
      }
      

      var regexResultCount = /(?:"totalResults":")(.*?)(?:")/gm;

      var r;
      while ((r = regexResultCount.exec(str)) !== null) {
          // This is necessary to avoid infinite loops with zero-width matches
          if (r.index === regexResultCount.lastIndex) {
              regexResultCount.lastIndex++;
          }
         
          // The result can be accessed through the `r`-variable.
          r.forEach((match, groupIndex) => {
              if(groupIndex ){
                  totalResults = match
                  pages = totalResults / 40
              }
          });
      }

      var dec = Math.ceil(pages)
      pages = (dec > 0) ? Math.floor(pages) + 1 : Math.floor(pages)

      for (var i = 1; i <= pages; i++) {
        if(i <= maxPage)
          rows.push(url+(keywords[currentKey].keyword).replace(/\\/g, '').replace(/ /g, '+')+'&page='+i)
        else
          break;
      }

      setTimeout(function(){
        chrome.tabs.query({ currentWindow: true, active: true }, function (tabs) {
          nextKeyword(tabs[0].id)
        })
      }, 2000)  
    }
  }
});

chrome.tabs.onUpdated.addListener( function (tabId, changeInfo, tab) {

  if (changeInfo.status == 'complete' && initiated) {

     var message = document.querySelector('#message');
     //alert(message)
    chrome.tabs.executeScript(null, {
      file: "getPagesSource.js"
    }, function() {
      
      // If you try and inject into an extensions page or the webstore/NTP you'll get an error
      if (chrome.runtime.lastError) {
        alert(chrome.runtime.lastError.message)
      }
    });

  }
})








