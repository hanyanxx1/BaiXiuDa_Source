$(function(){  
    function IsPC() {
    	var userAgentInfo = navigator.userAgent;
    	var Agents = ["Android", "iPhone",
    		"SymbianOS", "Windows Phone",
    		"iPad", "iPod"
    	];
    	var flag = true;
    	for (var v = 0; v < Agents.length; v++) {
    		if (userAgentInfo.indexOf(Agents[v]) > 0) {
    			flag = false;
    			break;
    		}
    	}
    	return flag;
    }
    
    var flag = IsPC(); //true为PC端，false为手机端
    if (flag == false) {
    	window.onresize = r;
    
    	function r(resizeNum) {
    		var winW = window.innerWidth;

    		document.getElementsByTagName("html")[0].style.fontSize = winW * (1 / 7.5) + "px";
    		// document.getElementsByTagName("html")[0].style.fontSize=winW*0.15625+"px";
    		if (winW > window.screen.width && resizeNum <= 10) {
    			setTimeout(function() {
    				r(++resizeNum)
    			}, 100);
    		} else {
    			document.getElementsByTagName("body")[0].style.opacity = 1;
    		}
    	};
    	setTimeout(function() {
    		r(0)
    	}, 100);
    } else {
    	//  监控窗口宽度变化
    	if ($(window).width() < 1030) { //  屏宽1330触发
    		window.onresize = r;
    
    		function r(resizeNum) {

    			var winW = window.innerWidth;
    			document.getElementsByTagName("html")[0].style.fontSize = winW * (1 / 7.5) + "px";
    			// document.getElementsByTagName("html")[0].style.fontSize=winW*0.15625+"px";
    			if (winW > window.screen.width && resizeNum <= 10) {
    				setTimeout(function() {
    					r(++resizeNum)
    				}, 100);
    			} else {
    				document.getElementsByTagName("body")[0].style.opacity = 1;
    			}
    		};
    		setTimeout(function() {
    			r(0)
    		}, 100);
    	} else {
    		window.onresize = r;
    
    		function r(resizeNum) {

    			var winW = window.innerWidth;
    			document.getElementsByTagName("html")[0].style.fontSize = '';
    			// document.getElementsByTagName("html")[0].style.fontSize=winW*0.15625+"px";
    			if (winW > window.screen.width && resizeNum <= 10) {
    				setTimeout(function() {
    					r(++resizeNum)
    				}, 100);
    			} else {
    				document.getElementsByTagName("body")[0].style.opacity = '';
    			}
    		};
    		setTimeout(function() {
    			r(0)
    		}, 100);
    	}
    }
    });  

$(window).resize(function() {
	//  监控窗口宽度变化
	if ($(window).width() < 1030) { //  屏宽1330触发
		window.onresize = r;

		function r(resizeNum) {

			var winW = window.innerWidth;
			document.getElementsByTagName("html")[0].style.fontSize = winW * (1 / 7.5) + "px";
			// document.getElementsByTagName("html")[0].style.fontSize=winW*0.15625+"px";
			if (winW > window.screen.width && resizeNum <= 10) {
				setTimeout(function() {
					r(++resizeNum)
				}, 100);
			} else {
				document.getElementsByTagName("body")[0].style.opacity = 1;
			}
		};
		setTimeout(function() {
			r(0)
		}, 100);
	} else {
		window.onresize = r;

		function r(resizeNum) {

			var winW = window.innerWidth;
			document.getElementsByTagName("html")[0].style.fontSize = '';
			// document.getElementsByTagName("html")[0].style.fontSize=winW*0.15625+"px";
			if (winW > window.screen.width && resizeNum <= 10) {
				setTimeout(function() {
					r(++resizeNum)
				}, 100);
			} else {
				document.getElementsByTagName("body")[0].style.opacity = '';
			}
		};
		setTimeout(function() {
			r(0)
		}, 100);
	}
});
