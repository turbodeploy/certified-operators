var bytesUploaded = 0;
var bytesTotal = 0;
var previousBytesLoaded = 0;
var intervalTimer = 0;
var myMessages = ['info', 'warning', 'error', 'success'];
var passInt = 0;
var currPass = '';
var passTimerActive = 0;
var radioValue = 'AUTHENTICATE';
var outputIntervalTimer = null;
var advMD = 0;
var vmtbuild = "Not Known";
var vmtrelease = "Not Known";
var vmtbits = "00";
var osbits = "00";
var allowUpdate = "true";
// callType is ACTION for doing, READ for output READ, DOWN for file download
var callType = "ACTION";
// globals for file dates
var d = new Date();
var currDate = ""; 
var currMonth = ""; 
var currYear = "";
var currHour = "";
var currMins = "";
var fileDate = "Unset"; 
var file = "";
var fileSize = 0;
// Set maxFileSize to match CGI limit
var maxFileSize = 1024*500000;
// progressFile is the output from the CGI
var progressFile = '';
// lastRun is flag to indicate final output update
// Set to falsy value if not
var lastRun = 0;


function GetXmlHttpObject() {
    var xmlHttp = null;
    try {
        // Firefox, Opera 8.0+, Safari
        xmlHttp = new XMLHttpRequest();
    }
    catch (e)
    {
        // Internet Explorer
        try
        {
            xmlHttp=new ActiveXObject("Msxml2.XMLHTTP");
        }
        catch (e)
        {
            xmlHttp=new ActiveXObject("Microsoft.XMLHTTP");
        }
    }
    return xmlHttp;
}

function generateFileName() {
    //Construct output file name
    // Called when user re-chooses radio button selection
    // Regenerates a new file prepend 
    d = new Date();
    currDate = d.getUTCDate().toString();
    // Make day two digits
    currDate = "0" + currDate;
    currDate = currDate.slice(-2);
    currMonth = d.getUTCMonth().toString();
    currMonth++; //Months start at zero
    // Make month two digits
    currMonth = "0" + currMonth;
    currMonth = currMonth.slice(-2);
    currYear = d.getUTCFullYear().toString();
    currHour = d.getUTCHours().toString();
    // Make hour two digits
    currHour = "00" + currHour;
    currHour = currHour.slice(-2);
    currMins = d.getUTCMinutes().toString();
    // Make mins two digits
    currMins = "00" + currMins;
    currMins = currMins.slice(-2);
    fileDate = currDate + "." + currMonth + "."  + currYear + "." + currHour + currMins;
    callType = "ACTION";
}

function hideAllMessages() {
    // Hide banner message pop-down               
    var messagesHeights = new Array(); // this array will store height for each message banner    
    for (i=0; i<myMessages.length; i++)
    {
                  messagesHeights[i] = $('.' + myMessages[i]).outerHeight(); // fill array
                  $('.' + myMessages[i]).css('top', -messagesHeights[i]); //move element outside viewport
    }
}

function showMessage(type) {
    //Display selected message
    hideAllMessages(); 
    $('.'+type).animate({top:"0"}, 500); //500 msec animation down
}

function fileSelected() {
    // Called when file has been chosen for upload
    file = document.getElementById('fileToUpload').files[0];
    fileSize = 0;
    // Check if we have a filename at all, and, if so, it is not too big
    if ((file) && (file.size <= maxFileSize)) {
        if (!advMD) {
            if (radioValue == "UPDATE") {
                // Not an Expert. Do checks.
                if (file.name.match(/\.zip$/i)) {
                    // Correct file type
                    // filenames should be update-xxx.zip or update64-xxx.zip
                    var is64 = file.name.search("^update64");
                    if (is64 >= 0) { 
                        if (osbits == "64") { checkRelease(); }
                        else { 
                            $("#errormsg.error.message").html("<CENTER><h3>This is a 64 bit appliance update file and you are on a 32 bit appliance!</h3></CENTER><CENTER><p>Please use the correct update file.</p><CENTER>");
                            showMessage('error');
                            setTimeout(function(){hideAllMessages()},7000); 
                            document.getElementById('fileToUpload').style.border = '2px solid #F00';
                            hide('upload');
                        }
                    }
                    else { 
                        // is it a 32 bit file?
                        var is32 = file.name.search("^update");
                        if (is32 >= 0) { 
                            if (osbits == "32") { checkRelease(); }
                            else { 
                                $("#errormsg.error.message").html("<CENTER><h3>This is a 32 bit appliance update file and you are on a 64 bit appliance!</h3></CENTER><CENTER><p>Please use the correct update file.</p><CENTER>");
                                showMessage('error');
                                setTimeout(function(){hideAllMessages()},7000); 
                                document.getElementById('fileToUpload').style.border = '2px solid #F00';
                                hide('upload');
                            } 
                        }
                        else { 
                            $("#errormsg.error.message").html("<CENTER><h3>This does not appear to be a valid appliance update zip file.</h3></CENTER><CENTER><p>Please choose an update file.</p><CENTER>");
                            showMessage('error');
                            setTimeout(function(){hideAllMessages()},7000); 
                            document.getElementById('fileToUpload').style.border = '2px solid #F00';
                            hide('upload');
                        }
                    }            
                }//end file type if
                else { 
                    //invalid file type
                            $("#errormsg.error.message").html("<CENTER><h3>Please choose a valid compressed update file.</h3></CENTER><CENTER><p>Select an update zip file you have downloaded from VMTurbo support.</p><CENTER>");
                            showMessage('error');
                            setTimeout(function(){hideAllMessages()},7000); 
                            document.getElementById('fileToUpload').style.border = '2px solid #F00';
                            hide('upload');
                }
                // Check if the user is allowed to update their appliance. This info is returned
                // with the version information.
                if (allowUpdate != "true") { 
                     $("#errormsg.error.message").html("<CENTER><h3>Update is not allowed due to exceeding the socket limit.</h3></CENTER><CENTER><p>Please contact VMTurbo support.</p><CENTER>");
                    showMessage('error');
                    setTimeout(function(){hideAllMessages()},7000); 
                    document.getElementById('fileToUpload').style.border = '2px solid #F00';
                    hide('upload');
                 }
            } //end radio value UPDATE
            else if ( radioValue == "BRANDING") {
                if (file.name == "branding.zip") { validFile(); }
                else {
                    $("#errormsg.error.message").html("<CENTER><h3>Please choose a valid rebranding file.</h3></CENTER><CENTER><p>The file should be called \"branding.zip\".</p><CENTER>");
                    showMessage('error');
                    setTimeout(function(){hideAllMessages()},7000); 
                    document.getElementById('fileToUpload').style.border = '2px solid #F00';
                    hide('upload');
                }
            }// end radioValue BRANDING
            else if ( radioValue == "INTEGRATE") {
                if (file.name == "action_scripts.zip") { validFile(); }
                else {
                    $("#errormsg.error.message").html("<CENTER><h3>Please choose a valid integration scripts file.</h3></CENTER><CENTER><p>The file should be called \"action_scripts.zip\".</p><CENTER>");
                    showMessage('error');
                    setTimeout(function(){hideAllMessages()},7000); 
                    document.getElementById('fileToUpload').style.border = '2px solid #F00';
                    hide('upload');
                }
            }// end radioValue INTEGRATE
        }//end not advanced
        else {
            // Advanced. Skip validation checks.
             validFile()
        }
    }//End file present and not too big
    else {
        if (file) { 
            // File is present, so file selected is too big
            // Tell the user about it (round down the max limit)
            $("#errormsg.error.message").html("<CENTER><h3>File Selected is too Large!</h3></CENTER><CENTER><p>The selected file is too large. There is a maximum file size limit of " + Math.floor(maxFileSize/1000000) + " MB</p><CENTER>");
            showMessage('error');
            setTimeout(function(){hideAllMessages()},5000); 
            document.getElementById('fileToUpload').style.border = '2px solid #F00';
            hide('upload');
        }
        else {
            // File is not present 
            document.getElementById('fileToUpload').style.border = '1px solid #CCC';
            hide('upload');
        }
    }
}
        
function checkRelease() {
    // Called when update zip file is valid type 
    // Checks release version against installed
    var startChar = file.name.search("-");
    startChar++;
    var endChar = file.name.search(".zip");    
    var updateFileVersionStr = file.name.substring(startChar, endChar);
    var updateFileVersion = parseInt(updateFileVersionStr);
    var applianceVersion = parseInt(vmtbuild);
    if ( updateFileVersion - applianceVersion > 0 ){
        // File is more recent than the appliance
        validFile();
    }
    else {
        // Version is the same or older than current
        $("#errormsg.error.message").html("<CENTER><h3>The update file you have chosen is the either older, or the same version as the current appliance.</h3></CENTER><CENTER><p>Please choose an update file which is newer than your current appliance.</p><CENTER>");
        showMessage('error');
        setTimeout(function(){hideAllMessages()},7000); 
        document.getElementById('fileToUpload').style.border = '2px solid #F00';
        hide('upload');
    }
        
}
        
function validFile() {
    // Called when the update file is validated
    document.getElementById('fileToUpload').style.border = '1px solid #CCC';
    unHide( 'upload' );
    if (file.size > 1024 * 1024)
      fileSize = (Math.round(file.size * 100 / (1024 * 1024)) / 100).toString() + 'MB';
    else
      fileSize = (Math.round(file.size * 100 / 1024) / 100).toString() + 'KB';    
    document.getElementById('fileInfo').style.display = 'block';
    document.getElementById('fileName').innerHTML = 'Name: ' + file.name;
    document.getElementById('fileSize').innerHTML = 'Size: ' + fileSize;
    document.getElementById('fileType').innerHTML = 'Type: ' + file.type;
}
  
function uploadFile() {
    // Start upload of selected file, and trigger update of progress information
    previousBytesLoaded = 0;
    unHide('infopanel');
    unHide('fileStats');
    document.getElementById('uploadResponse').style.display = 'none';
    document.getElementById('progressNumber').innerHTML = '';
    var progressBar = document.getElementById('progressBar');
    progressBar.style.display = 'block';
    progressBar.style.width = '0px';        
    var fd = new FormData();
    fd.append("userName", document.getElementById('userName').value);
    fd.append("password", document.getElementById('password').value);
    fd.append("actionType", radioValue);
    fd.append("fileDate", fileDate);
    //fd.append("callType", callType);
    fd.append("callType", "ACTION");
    fd.append("fileToUpload", document.getElementById('fileToUpload').files[0]);
    xhr=GetXmlHttpObject();
    if (xhr==null) {
        alert ("Your browser does not support HTML5/AJAX!\n*** You should update your browser! ***\nFor Internet Explorer 10, check ActiveX is enabled\nRedirecting you to a basic version of this page.");
        location.replace('oldupdate.html');
    } 
    xhr.upload.addEventListener("progress", uploadProgress, false);
    xhr.addEventListener("load", uploadComplete, false);
    xhr.addEventListener("error", uploadFailed, false);
    xhr.addEventListener("abort", uploadCanceled, false);
    xhr.open("POST", "/cgi-bin/vmtadmin.cgi");
    xhr.send(fd);
    // Switch in correct CSS class to indicate busy button and change label
    // Unbind default click event handler, and add new event handler
    // Use both removeattr and unbind to work around jquery bug
	$('input#upload').removeClass('activated').addClass('busy').prop('value', 'Cancel!').removeAttr("onclick").unbind("click").click( function() { xhr.abort() });
    // Disable Radio Buttons to prevent mid-flight change
    $("#chooser").attr("disabled",true);
    // Hide any old messages
    hideAllMessages();
    //update stats every 1 second
    intervalTimer = setInterval(updateTransferSpeed, 1000);
}

function downloadFile() {
    // Process download request
    previousBytesLoaded = 0;
    unHide( 'downloadinfopanel' );     
    var dfd = new FormData();
    dfd.append("userName", document.getElementById('userName').value);
    dfd.append("password", document.getElementById('password').value);
    dfd.append("actionType", radioValue);
    dfd.append("fileDate", fileDate);
    dfd.append("callType", "ACTION");
  
    dxhr=GetXmlHttpObject();
    if (dxhr==null)
    {
        alert ("Your browser does not support HTML5/AJAX!\n*** You should update your browser! ***\nFor Internet Explorer 10, check ActiveX is enabled\nRedirecting you to a basic version of this page.");
        location.replace('oldupdate.html');
    } 
    dxhr.addEventListener("load", downloadReady, false);
    dxhr.addEventListener("error", downloadFailed, false);
    dxhr.addEventListener("abort", downloadCanceled, false);
    dxhr.open("POST", "/cgi-bin/vmtadmin.cgi");
    dxhr.send(dfd);
    $('input#download').removeClass('activated').addClass('busy').prop('value', 'Cancel!').removeAttr("onclick").unbind("click").click( function() { dxhr.abort() });
    var downloadResponse = document.getElementById('downloadResponse');
    unHide( 'downloadResponse' );    
    downloadResponse.innerHTML = '<span style="font-size: 18pt; font-weight: bold;">Please wait...</span>';
    downloadResponse.style.display = 'block';
    // Disable Radio Buttons to prevent mid-flight change
    $("#chooser").attr("disabled",true);
    // Hide any old messages
    hideAllMessages();
    //update output every half second
    outputIntervalTimer = "";
    outputIntervalTimer = setInterval(function(){updateOutputDiv("#downloadResponse");}, 500);
}

function updateTransferSpeed() {
    var currentBytes = bytesUploaded;
    var bytesDiff = currentBytes - previousBytesLoaded;
    if (bytesDiff == 0) return;
    previousBytesLoaded = currentBytes;
    var bytesRemaining = bytesTotal - previousBytesLoaded;
    var secondsRemaining = bytesRemaining / bytesDiff;    
    var speed = "";
    if (bytesDiff > 1024 * 1024)
        speed = (Math.round(bytesDiff * 100/(1024*1024))/100).toString() + 'MBps';
    else if (bytesDiff > 1024)
        speed =  (Math.round(bytesDiff * 100/1024)/100).toString() + 'KBps';
    else
        speed = bytesDiff.toString() + 'Bps';
        document.getElementById('transferSpeedInfo').innerHTML = speed;
        document.getElementById('timeRemainingInfo').innerHTML = '| ' + secondsToString(secondsRemaining);        
}

function secondsToString(seconds) {        
    var h = Math.floor(seconds / 3600);
    var m = Math.floor(seconds % 3600 / 60);
    var s = Math.floor(seconds % 3600 % 60);
    return ((h > 0 ? h + ":" : "") + (m > 0 ? (h > 0 && m < 10 ? "0" : "") + m + ":" : "0:") + (s < 10 ? "0" : "") + s);
}

function uploadProgress(evt) {
    if (evt.lengthComputable) {
        bytesUploaded = evt.loaded;
        bytesTotal = evt.total;
    if (bytesTotal > 1024*1024)
        document.getElementById('fileSize').innerHTML = 'Size: ' + (Math.round(bytesTotal * 100/(1024*1024))/100).toString() + 'MB';
    else if (bytesUploaded > 1024)
        document.getElementById('fileSize').innerHTML = 'Size: ' + (Math.round(bytesTotal * 100/1024)/100).toString() + 'KB';
    else
        document.getElementById('fileSize').innerHTML = 'Size: ' +(Math.round(bytesTotal * 100)/100).toString() + 'Bytes';
        var percentComplete = Math.round(evt.loaded * 100 / evt.total);
        var bytesTransfered = '';
    if (bytesUploaded > 1024*1024)
        bytesTransfered = (Math.round(bytesUploaded * 100/(1024*1024))/100).toString() + 'MB';
    else if (bytesUploaded > 1024)
        bytesTransfered = (Math.round(bytesUploaded * 100/1024)/100).toString() + 'KB';
    else
        bytesTransfered = (Math.round(bytesUploaded * 100)/100).toString() + 'Bytes';    
        document.getElementById('progressNumber').innerHTML = percentComplete.toString() + '%';
        document.getElementById('progressBar').style.width = (percentComplete * 3.55).toString() + 'px';
        document.getElementById('transferBytesInfo').innerHTML = bytesTransfered;
        if (percentComplete > 98) {  
            // Stop calling this function with the timer
            clearInterval(intervalTimer);
            var uploadResponse = document.getElementById('uploadResponse');
            uploadResponse.style.display = 'block';
            if (!outputIntervalTimer){ 
                // Needed because this code can occasionally be called twice by other timer
            outputIntervalTimer = setInterval(function(){updateOutputDiv("#uploadResponse");}, 500);
            }
        }
    }
    else {
        document.getElementById('progressBar').innerHTML = 'unable to compute';
    }  
}

function uploadComplete(evt) {
    // Event handler triggered when upload action completed
    // Stop updating progress bar info
    //clearInterval(intervalTimer);
    var uploadResponse = document.getElementById('uploadResponse');
    //uploadResponse.innerHTML = evt.target.responseText;
    uploadResponse.style.display = 'block';
    // Stop updating the output div
    clearInterval(outputIntervalTimer);
    outputIntervalTimer = null;
    // Call outputdiv Updater one last time to get last entry
    lastRun = 1;
    updateOutputDiv("#uploadResponse");
    // Enable Radio Buttons again
    $("#chooser").removeAttr('disabled');
    // Hide submit button until a new file choice is made
    hide("upload");
    hide("fileStats");
    $('input#upload').removeClass('busy').addClass('activated').prop('value', 'Upload').removeAttr("onclick").unbind("click").click( function() { uploadFile() });
    showMessage('success');
    setTimeout(function(){hideAllMessages()},3000);
}  

function downloadReady(evt) {
    // Event handler triggered when download action completed
    // Check type of download, and set CGI arguments accordingly
    // Then set the src of download target to that CGI
    // CGI will send correct file back
    clearInterval(intervalTimer);
    var downloadFrame = document.getElementById('downloadTarget');
    // TODO: Passback of filename from CGI    
    //downloadFrame.href = 'CA.crt';
    var userName = document.getElementById('userName').value;
    var password = document.getElementById('password').value;
    downloadFrame.src = "/cgi-bin/vmtadmin.cgi?userName="+userName+"&password="+password+"&callType=DOWN&actionType=" + radioValue +"&fileDate=" + fileDate;
    // Stop updating the output div
    clearInterval(outputIntervalTimer);
    outputIntervalTimer = null;
    // Call outputdiv Updater one last time to get last entry
    updateOutputDiv("#downloadResponse");
    // Enable Radio Buttons again
    $("#chooser").removeAttr('disabled');
    $('#download').removeClass('busy').addClass('activated').prop('value', 'Download').removeAttr("onclick").unbind("click").click( function() { downloadFile() });
    hide("filedown");
    //showMessage('success');
    setTimeout(function(){hideAllMessages()},3000);
}  

function updateOutputDiv(divName) {
    // Called by a timer interval to update in-progress div
    // Ajax call to read the output text file
    // Call stats update one last time (for edge case 99pct/100pct
    updateTransferSpeed();
    userName = "DummyUser";
    password = "DummyPassword";
    callType = "READ";
    //var actionType = "VERSIONS";
    $.ajax({
        type: "POST",
        url: "/cgi-bin/vmtadmin.cgi", // URL of the CGI
        dataType: "text",
        // send dummy UID + PWD to CGI
        data: "userName=" + userName + "&password=" + password +"&actionType=" + radioValue +"&fileDate=" + fileDate +"&callType=" + callType,
        
        error: function(XMLHttpRequest, textStatus, errorThrown) { 
            // script call was *not* successful
            $("#warnmsg.warning.message").html("<CENTER><h3>Error Reading Progress Update File!</h3></CENTER><CENTER><p>Expected update file \"" + progressFile + "\" - Your task may still complete.<br> Wait for further status messages for the task status.</p><CENTER>");
            showMessage('warning');
            // Prevent meltdown of browser by cancelling this functions timer
            clearInterval(outputIntervalTimer);
            outputIntervalTimer = null;
            setTimeout(function(){hideAllMessages()},7000); 
        }, 
        success: function(data){
            // script call was successful 
            $(divName).html(data.replace(/\n/g,'<br />'));
            if (lastRun) {
                // Last call, action has completed
                // Append pop-out div after 1 second
                setTimeout(function(){makePopup(divName)},1000);
                // Set back to falsy value
                // in case multiple actions
                lastRun = 0;
            }
        } 
    }); 
}  

function makePopup(divName) {
    // Called after output is completed
    // Appends link to allow pop-out of
    // output text into seperate window
    var $newLink = "";
    $newLink = $('<a href="#" id="popOut" name="popOut">Show this output in a separate window</a>');
    // 79 Characters length of append
    // remove click handlers in case of multi upload in single session
    $(divName).append($newLink);
    $('#popOut').removeAttr("onclick").unbind("click").click( function() { showPopup() }); 
}

function showPopup() {
    var divContent = $('#uploadResponse').html();
    // remove appended text - last 79 characters
    divContent = divContent.substring(0,divContent.length-79);
    //calculate height of popout window to match output length (max 600)
    var newHeight = $('#uploadResponse').height()+75;
    newHeight = ((newHeight > 600) ? 600 : newHeight);
    newwindow2=window.open('','name','height='+ newHeight +',width=700, location=no, scrollbars=yes');
	var tmp = newwindow2.document;
	tmp.write('<html><head><title>VMTurbo Operations Manager Administration - Output Results</title>');
	tmp.write('<link rel="stylesheet" type="text/css" href="update.css">');
	tmp.write('</head><body id="popup">');
    tmp.write('<p>' + divContent + '</p>');
    tmp.write('<CENTER><input type="button"  class="close" onClick="self.close()" value="Close"/></CENTER>');
	tmp.write('</body></html>');
	tmp.close();
    
}

function authenticateComplete(aevt) {
    // Reset button
    $('#authenticate').removeClass('busy').addClass('activated').prop('value', 'Authenticate').removeAttr("onclick").unbind("click").click( function(event) { checkDetails(event) });
    if (aevt.target.responseText=="SUCCESS")
    {
        // display ticks, disable inputs and button
        $("#userOK").css("visibility","visible");
        $("#passOK").css("visibility","visible");
        $("#authenticate").attr("disabled","true");
        // hide button as it is no longer needed
        $("#authenticate").hide();
        $("#userName").attr("disabled","true");
        $("#password").attr("disabled","true");
        // Set Expert mode if modifier used
        if (advMD)
        {
            // Shift Modifier is active
            $("#firstH1").html("Expert Mode");
            $("#check").show();
            $("#checklabel").show();
            $("#uptodate").show();
            $("#uptodatelabel").show();
            $("#fullbackup").show();
            $("#fullbackuplabel").show();
            $("#cfgrestore").show();
            $("#cfgrestorelabel").show();
            $("#fullrestore").show();
            $("#fullrestorelabel").show();
            $("#exportbackup").show();
            $("#exportbackuplabel").show();
            $("#expertfile").show();
            $("#expertfilelabel").show();
            $("#exportdiags").show();
            $("#exportdiagslabel").show();
            $("#expertdiags").show();
            $("#expertdiagslabel").show();
        }
        // enable next div 
        unHide("chooser");
    }
	if (aevt.target.responseText=="FAIL")
	{
        $("#errormsg.error.message").html("<CENTER><h3>This username and password combination is not valid!</h3></CENTER><CENTER><p>Try a different one</p><CENTER>");
        showMessage('error');
        setTimeout(function(){hideAllMessages()},5000);
        // Set inputs to red and refocus to username input	
        document.getElementById('userName').style.border = '2px solid #F00';
        document.getElementById('password').style.border = '2px solid #F00';
        $("#userName").focus();
	}
	if (aevt.target.responseText=="NOPRIV")
	{
        currUser=$("#userName").val();
        $("#warnmsg.warning.message").html("<CENTER><h3>User \"<em>"+currUser+"</em>\" does not have administration rights!</h3></CENTER><CENTER><p>Please enter a user with administration privileges</p></CENTER>");
        showMessage('warning');
        setTimeout(function(){hideAllMessages()},5000);
        $("#userName").focus();
	}
}  

function uploadFailed(evt) {
    clearInterval(intervalTimer);
    $("#errormsg.error.message").html("<CENTER><h3>An error has occurred during the upload!</h3></CENTER><CENTER><p>Please reload the page and retry.</p><CENTER>");  
    showMessage('warning');
    setTimeout(function(){hideAllMessages()},10000);
    //clear file info and progress
    radioToggle("UPLOAD");
    // Enable Radio Buttons
    $("#chooser").removeAttr('disabled');
    // Prevent meltdown of browser by cancelling output Div timer
    clearInterval(outputIntervalTimer);
    outputIntervalTimer = null;
    // Swap back button class and action to active
    $('#upload').removeClass('busy').addClass('activated').prop('value', 'Upload').removeAttr("onclick").unbind("click").click( function() { uploadFile() });
}  
  
function uploadCanceled(evt) {
    clearInterval(intervalTimer);
    $("#warnmsg.warning.message").html("<CENTER><h3>The upload has been cancelled, or the browser dropped the connection.</h3></CENTER><CENTER><p>If your browser dropped the connection, please reload the page and retry.</p></CENTER>"); 
    showMessage('warning');
    setTimeout(function(){hideAllMessages()},5000);
    //clear file info and progress
    radioToggle("UPLOAD");
    // Enable Radio Buttons
    $("#chooser").removeAttr('disabled');
    // Prevent meltdown of browser by cancelling output Div timer
    clearInterval(outputIntervalTimer);
    outputIntervalTimer = null;
    // Swap back button class and action to active
    $('#upload').removeClass('busy').addClass('activated').prop('value', 'Upload').removeAttr("onclick").unbind("click").click( function() { uploadFile() });
}

function authFailed(evt) {
    // called when XHR fails, not authentication itself
    $("#errormsg.error.message").html("<CENTER><h3>An error has occurred while attempting to authenticate!</h3></CENTER><CENTER><p>Please reload the page and retry.</p><CENTER>");  
    showMessage('warning');
    setTimeout(function(){hideAllMessages()},10000);
    // Enable Radio Buttons
    $("#chooser").removeAttr('disabled');
    // Swap back button class and action to active
    $('#authenticate').removeClass('busy').addClass('activated').prop('value', 'Authenticate').removeAttr("onclick").unbind("click").click( function(event) { checkDetails(event) });
}  
  
function authCanceled(evt) {
    // called when auth XHR is aborted or cancelled by browser
    clearInterval(intervalTimer);
    $("#warnmsg.warning.message").html("<CENTER><h3>Authentication cancelled, or the browser dropped the connection.</h3></CENTER><CENTER><p>If your browser dropped the connection, please reload the page and retry.</p></CENTER>"); 
    showMessage('warning');
    setTimeout(function(){hideAllMessages()},5000);
    // Enable Radio Buttons
    $("#chooser").removeAttr('disabled');
    // Swap back button class and action to active
    $('#authenticate').removeClass('busy').addClass('activated').prop('value', 'Authenticate').removeAttr("onclick").unbind("click").click( function(event) { checkDetails(event) });
}  

function downloadFailed(evt) {
    clearInterval(intervalTimer);
    $("#errormsg.error.message").html("<CENTER><h3>An error has occurred during the download!</h3></CENTER><CENTER><p>Please reload the page and retry.</p><CENTER>");  
    showMessage('warning');
    setTimeout(function(){hideAllMessages()},10000);
    //clear file info and progress
    radioToggle("DOWNLOAD");
    // Enable Radio Buttons
    $("#chooser").removeAttr('disabled');
     // Prevent meltdown of browser by cancelling output Div timer
    clearInterval(outputIntervalTimer);
    outputIntervalTimer = null;
    $('#download').removeClass('busy').addClass('activated').prop('value', 'Download').removeAttr("onclick").unbind("click").click( function() { downloadFile() });
}  
  
function downloadCanceled(evt) {
    clearInterval(intervalTimer);
    $("#warnmsg.warning.message").html("<CENTER><h3>The download has been cancelled, or the browser dropped the connection.</h3></CENTER><CENTER><p>If your browser dropped the connection, please reload the page and retry.</p></CENTER>"); 
    showMessage('warning');
    setTimeout(function(){hideAllMessages()},5000);
    //clear file info and progress
    radioToggle("DOWNLOAD");
    // Enable Radio Buttons
    $("#chooser").removeAttr('disabled');
     // Prevent meltdown of browser by cancelling output Div timer
    clearInterval(outputIntervalTimer);
    outputIntervalTimer = null;
    $('#download').removeClass('busy').addClass('activated').prop('value', 'Download').removeAttr("onclick").unbind("click").click( function() { downloadFile() });
}  

function checkDetails(evt) {
    //authenticate user pass details and permissions
    // if OK, enable next div, or display error
    // processing happens via event handler functions
    //Get Key modifier status for shift key on button
    // Hide any old messages
    hideAllMessages();
    //advMD = evt.shiftKey;
    if ( evt.ctrlKey || evt.metaKey ) { advMD = true; }
    var afd = new FormData();
    afd.append("userName", document.getElementById('userName').value);
    afd.append("password", document.getElementById('password').value);
    afd.append("actionType", "AUTHENTICATE");
    afd.append("callType", callType);
    afd.append("fileDate", fileDate);
    // Set inputs to grey border while we check them
    document.getElementById('userName').style.border = '1px solid #CCC';
    document.getElementById('password').style.border = '1px solid #CCC';
    axhr=GetXmlHttpObject();
    if (axhr==null)
    {
        alert ("Your browser does not support HTML5/AJAX!\n*** You should update your browser! ***\nFor Internet Explorer 10, check ActiveX is enabled\nRedirecting you to a basic version of this page.");
        location.replace('oldupdate.html');
    } 
    //No progress handler required;
    axhr.addEventListener("load", authenticateComplete, false);
    axhr.addEventListener("error", authFailed, false);
    axhr.addEventListener("abort", authCanceled, false);
    axhr.open("POST", "/cgi-bin/vmtadmin.cgi");
    axhr.send(afd);
    $('input#authenticate').removeClass('activated').addClass('busy').prop('value', 'Cancel!').removeAttr("onclick").unbind("click").click( function() { axhr.abort() });
}

function getRadioValue() {
    // Called when a different radio button is selected, and initial selection
    radioValue=$("input[name=updatetype]:checked").val(); 
    switch (radioValue) {
    	case 'UPDATE': radioToggle('UPLOAD'); generateFileName(); break;
        case 'CHECK': radioToggle('DOWNLOAD'); generateFileName(); break;
        case 'UPTODATE': radioToggle('DOWNLOAD'); generateFileName(); break;
        case 'BRANDING': radioToggle('UPLOAD'); generateFileName();break;
        case 'INTEGRATE': radioToggle('UPLOAD'); generateFileName();break;
        case 'EXPERTFILE': radioToggle('UPLOAD'); generateFileName();break;
        case 'GETBRAND': radioToggle('DOWNLOAD'); generateFileName();break;
        case 'GETINTEGRATE': radioToggle('DOWNLOAD'); generateFileName();break;
        case 'FULLBACKUP': radioToggle('DOWNLOAD'); generateFileName();break;
        case 'CFGBACKUP': radioToggle('DOWNLOAD'); generateFileName();break;
        case 'EXPORTBACKUP': radioToggle('DOWNLOAD'); generateFileName();break;
        case 'FULLRESTORE': radioToggle('UPLOAD'); generateFileName();break;
        case 'CFGRESTORE': radioToggle('UPLOAD'); generateFileName();break;
        case 'EXPORTDIAGS': radioToggle('DOWNLOAD'); generateFileName();break;
        case 'EXPERTDIAGS': radioToggle('DOWNLOAD'); generateFileName();break;
        default: throw new Error("Invalid Choice!");
    }
}

function radioToggle(actiontype) {
    // Swap around panel 3 when called with new radio selection made
    clearOutput();
    document.getElementById('fileToUpload').style.border = '1px solid #CCC';
    hide('upload'); 
    hide('infopanel'); 
    hide('downloadinfopanel');
    if (actiontype=="UPLOAD") {
        hide('filedown');
        unHide('filechoice');
    }
    else if (actiontype=="DOWNLOAD") {
        hide('filechoice');
        unHide('filedown');
    }
    else {
        throw new Error("Unexpected radiotoggle type!");
    }
}
               
function unHide(target) {
    // Called to unhide an object/div on the form
    var unHideObject = document.getElementById(target);
    unHideObject.disabled =false;
    unHideObject.style.zoom = '1';
    unHideObject.style.display = 'block';
    unHideObject.style.visibility = 'visible';
    unHideObject.style.opacity = '1';
}

function hide(target) {
    // Called to hide an object/div on the form
    var hideObject = document.getElementById(target);
    hideObject.disabled =true;
    hideObject.style.zoom = '1';
    hideObject.style.display = 'none';
    hideObject.style.visibility = 'hidden';
    hideObject.style.opacity = '0';
}

function clearOutput() {
    // Clear down output div and progress info
    // Used if something is cancelled, and need to reset
    $("#fileToUpload").val("");
    $("#uploadResponse").text("");
    $("#downloadResponse").text("");
    $("#fileSize").text("");
    $("#fileName").text("");
    $("#fileType").text("");
    $("#transferSpeedInfo").text("");
    $("#timeRemainingInfo").text("");
    $("#transferBytesInfo").text("");
}

function passMon(){
    // Fire password change check every 100ms
    // Can't rely on events due to paste
    // this in turn enables submit button
    if ( !passTimerActive ) {
        passTimerActive=1;
        passInt=self.setInterval("checkPassChange()",100);
    }
}

function stopMon() {
    // Halt password check timer because
    // control has lost focus, stop checking 
    self.clearInterval(passInt);
    passTimerActive=0;
}

function checkPassChange() {
    // User can change password field lots of ways, we need to check it
    var newPass = document.getElementById("password").value;
    if (newPass != currPass) {
        currPass = newPass;
        unHide( "authenticate" );
        // Switch in correct CSS class to indicate active button
        $('input#authenticate').removeClass('inactive').addClass('activated');
    }
}

function getVmtInfo() {
    // Called on document load to populate versioning information in postit
    // Info can also be used for version check on upload file
    var userName = "DummyUser";
    var password = "DummyPassword";
    var actionType = "VERSIONS";
    $.ajax({
        type: "POST",
        url: "/cgi-bin/vmtadmin.cgi", // URL of the CGI
        dataType: "text",
        // send dummy UID + PWD to CGI
        data: "userName=" + userName + "&password=" + password +"&actionType=" + actionType +"&fileDate=" + fileDate +"&callType=" + callType,
        
        error: function(XMLHttpRequest, textStatus, errorThrown) { 
            // script call was *not* successful
            //TODO: Nicer error
            // As this is first place CGI is called, uncomment below for verbose error logging to browser
            //alert("Script Call Error: \"" + errorThrown + "\"" + "TextStatus: \"" + textStatus + "\"" + XMLHttpRequest.responseText);
        }, 
        success: function(data){
            // script call was successful 
            // data contains the text value pairs returned by CGI
            // Split it into an object
            var splitPairs = data.split(",");
            var vmtbuildPair = splitPairs[0].split(":");
            var vmtreleasePair = splitPairs[1].split(":");
            var vmtbitsPair = splitPairs[2].split(":");
            var osbitsPair = splitPairs[3].split(":");
            var allowUpdatePair = splitPairs[4].split(":");
            vmtbuild = vmtbuildPair[1];
            vmtrelease = vmtreleasePair[1];
            vmtbits = vmtbitsPair[1];
            osbits = osbitsPair[1];
            allowUpdate = allowUpdatePair[1];
            $("#vmtbuild").empty().html("VMTurbo Build Number: "+vmtbuild+"<br />");
            $("#vmtrelease").empty().html("VMTurbo Release: "+vmtrelease+"<br />");
            $("#vmtbits").empty().html("VMTurbo Type: "+vmtbits+" bit<br />");
            $("#osbits").empty().html("Operating System Type: "+osbits+" bit<br />");
            $("#allowUpdate").empty().html("Allow Update: "+allowUpdate+"<br />");
        } 
    }); 
}

function keyMap() {
    //Simulate tab button and move to password if user hits enter in username field
    $("#userName").keyup(function(event){ if(event.keyCode == 13){ $("input#password").focus();} });
}

$(document).ready(function(){
    // Deal with none-js enabled browsers
    // Next line for very slow IE browsers to prevent default "NoJS" message in background causing confusion
    document.getElementById('noJSMsg').style.visibility='hidden';
    // manipulate DOM to reveal content for JS enabled browsers
    $("body").removeClass("noJS");
    // Hide all message banners 
    hideAllMessages();
    if (!jQuery.support.ajax) {
           // no AJAX support, use fallback
           alert("Your browser does not appear to support AJAX.\nTry a more modern browser (FireFox, Chrome, IE10).\nRedirecting you to a more basic version of this page");
           location.replace('oldupdate.html');
	   }
    // We have JS, AJAX, and now finally check for fileAPI support
    // Define test for fileAPI
        var fileAPISupport = (function(undefined) {
            return $("<input type='file'>")    // create test element
            .get(0)               // get native element
            .files !== undefined; // check whether files property is not undefined
    })();
    // Check result of fileAPI test
    if (!fileAPISupport) {
        // Browser has JS, AJAX, but no fileAPI
        // Probably old version of FF, Chrome, Safari
        alert("Your browser is outdated - please upgrade to the latest (FireFox 6+, Chrome 13+, IE10+), and re-vist this page.\n\nFor Internet Explorer 10+, check ActiveX is enabled.\nFor now, redirecting you to a basic version of this page.");
        location.replace('oldupdate.html');
    }
    // Update versioning information
    getVmtInfo();
    // When message banner is clicked, hide it
    $('.message').click(function(){
        $(this).animate({top: -$(this).outerHeight()}, 500);
    });            
    // Select Text in password and username filds when focus gained
    $("#userName").focus(function(){
        // Select input field contents
        this.select();
    });
    $("#password").focus(function(){
        // Select input field contents
        this.select();
    });
    //Simulate tab button and move to password if user hits enter in username field
    // Fire in 2 seconds to debounce from when user hits enter in URL
    setTimeout("keyMap()", 2000);
    //Simulate click of 'Authenticate' button if user hits enter in password field
    $("#password").keyup(function(event){
        if(event.keyCode == 13){
            $("input#authenticate").focus();
            $("input#authenticate").click();
        }
    });
    // Bind event handler to download submit button using JS instead of inline
    $('input#upload').unbind("click").click( function() { uploadFile() });
    // Bind event handler to authenticate submit button using JS instead of inline
    $('input#authenticate').unbind("click").click( function(event) { checkDetails(event) });
    // Bind event handler to authenticate submit button using JS instead of inline
    $('input#download').unbind("click").click( function() { downloadFile() }); 
    // Version details popup 
    // select all the a tag with name equal to modal
    $('a[name=modal]').click(function(e) {
        //Cancel the link behavior
        e.preventDefault();
        //Get the A tag
        var id = $(this).attr('href');    
        //Get the screen height and width
        var maskHeight = $(document).height();
        var maskWidth = $(window).width(); 
        //Set height and width to mask to fill up the whole screen
        $('#mask').css({'width':maskWidth,'height':maskHeight});  
        //transition effect    
        $('#mask').fadeIn(1000);   // 1 second fadein for mask
        $('#mask').fadeTo("slow",0.8);     
        //Get the window height and width
        var winH = $(window).height();
        var winW = $(window).width();
        //Set the popup window to center
        $(id).css('top',  winH/2-$(id).height()/2);
        $(id).css('left', winW/2-$(id).width()/2);    
        //transition effect
        $(id).fadeIn(1200); // 1.2 seconds for popup    
    });    
    //if close button is clicked
    $('.window .close').click(function (e) {
        //Cancel the link behavior
        e.preventDefault();
        $('#mask, .window').hide();
    });       
    //if bg mask is clicked hide popup as well
    $('#mask').click(function () {
        $(this).hide();
        $('.window').hide();
    });         
    // Recenter Post It with browser if browser window resize happens    
    $(window).resize(function () {    
        var box = $('#boxes .window');    
        //Get the screen height and width
        var maskHeight = $(document).height();
        var maskWidth = $(window).width();      
        //Set height and width to mask to fill up the whole screen
        $('#mask').css({'width':maskWidth,'height':maskHeight});           
        //Get the new window height and width
        var winH = $(window).height();
        var winW = $(window).width();   
        //Set the popup window to center
        box.css('top',  winH/2 - box.height()/2);
        box.css('left', winW/2 - box.width()/2);        
    });  
});

