(function () {
        var xmlHttp;
        var loaded = false;
        var startTime;
        var failed = false;

        // Interval for checking that we can reach the API component.
        var apiCheckInterval = null;

        // Interval for getting the status and updating the counter.
        var statusUpdateInterval = null;

        function doCheck() {
            if (loaded) {
                return;
            }

            var httpReq = new XMLHttpRequest();
            httpReq.onreadystatechange = function() {
                // If the request completed, and the status is "success", return.
                // We don't care what the response it - the fact that the call succeeded
                // means we reached the API component and it's up.
                if (httpReq.readyState == 4 && httpReq.status == 200) {
                    success();
                }
            }

            // We need to use some API that does not require authentication.
            // checkInit checks if the administrative user is initialized.
            httpReq.open("GET", window.location.origin + "/api/v3/checkInit");
            httpReq.send( null );
        }

        // Begins the process of checking whether the API component is up.
        function startApiCheck() {
            if (apiCheckInterval == null) {
                apiCheckInterval = setInterval(doCheck, 5000);
            }

            if (statusUpdateInterval == null) {
                statusUpdateInterval = setInterval(getStatus, 1000);
            }
        }

        // Called when we get a successful request through to the API component.
        // We can assume it's ready to go.
        function success() {
            loaded = true;
            if (statusUpdateInterval !== null) {
               clearInterval(statusUpdateInterval);
            }
            if (apiCheckInterval !== null) {
               clearInterval(apiCheckInterval);
            }
            setTimeout(function () {
                successMsg();
            }, 10000);

            setTimeout(function () {
                window.location.href = window.location.origin;
            }, 10000);
            // We are done
            document.getElementById("circle_div").className = 'circle_static';

            setTimeout(function () {
                startFading();
            }, 1000);
        }

        function successMsg() {
            document.getElementById("message").innerHTML = "Switching to login page...";
            setTimeout(function () {
                successMsg();
            }, 10000);
        }

        function startFading() {
            document.getElementById("circle_div").style.borderColor = '#f2f2f2';
            document.getElementById("circle_div").style.transition = 'border-color 15s linear';
            document.getElementById("time").style.color = '#f2f2f2';
            document.getElementById("time").style.transition = 'color 5s linear';
            setTimeout(function () {
                displayLogo();
            }, 5000);
        }

        function displayLogo() {
            time.style.display = 'none';
            time.style.visibility = 'hidden';
            logo.style.display = 'block';
            logo.style.visibility = 'visible';
        }

        function onLoad() {
            startTime = new Date();
            document.getElementById("time").innerHTML = getTimestamp();
            document.getElementById("message").innerHTML = "Initializing Turbonomic XL...";
            startApiCheck();
        }

        function getStatus() {
            xmlHttp = new XMLHttpRequest();
            xmlHttp.onreadystatechange = getResponse;
            xmlHttp.open("GET", window.location.origin +"/status", true);
            xmlHttp.send(null);
        }

        function normalizeTime(param) {
            if (param < 10) {
                param = "0" + param;
            }
            return param
        }
        function getTimestamp() {
            var x = (new Date() - startTime) / 1000;
            var sec = Math.floor(x % 60);
            x /= 60;
            var min = Math.floor(x % 60);
            var m = normalizeTime(min);
            var s = normalizeTime(sec);
            return m + ":" + s;
        }

        function failSetup() {
            document.getElementById("time").innerHTML = "FAIL";
            document.getElementById("circle_div").style.borderColor = 'red';
        }

        function getResponse() {
            // If we've failed, just return.
            if (failed) {
                return;
            }

            document.getElementById("time").innerHTML = getTimestamp();
            if (xmlHttp.readyState == 4 && xmlHttp.status == 200) {
                var status;
                // We have a failure.
                if (xmlHttp.responseText.substring(0, 3) == "!! ") {
                    failed = true;
                    status = xmlHttp.responseText.substring(3);
                    document.getElementById("circle_div").className = 'circle_static';
                    setTimeout(function () {
                        failSetup();
                    }, 10);
                } else {
                    status = xmlHttp.responseText;
                }

                document.getElementById("message").innerHTML = status;
            }
        }

        onLoad();
})(this);