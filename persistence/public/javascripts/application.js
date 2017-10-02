// Place your application-specific JavaScript functions and classes here
// This file is automatically included by javascript_include_tag :defaults

//
// Sample usage:
//
// <input type="submit" name="commit" value="Delete" onClick="return confirmSubmit()">
//

function confirmSubmit(msg)
{
    var agree = confirm(msg ? msg : "Are you sure?");
    if (agree)
        return true ;
    else
        return false ;
}

function selectToggle(toggle,form_name) {
    var myForm = document.forms[form_name];
    for( var i=0; i < myForm.length; i++ ) { 
        if(toggle) {
            myForm.elements[i].checked = "checked";
        } 
        else {
            myForm.elements[i].checked = "";
        }
    }
}

//BEGIN-UID.user_methods

//END-UID.user_methods

