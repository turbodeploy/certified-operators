function wrap(longStr,width){
length = longStr.length;
if(length <= width)
return longStr;
return (longStr.substring(0, width) + "\n" +
wrap(longStr.substring(width, length), width));
}