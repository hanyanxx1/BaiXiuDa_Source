//JsSIP.debug.enable('JsSIP:*');
var debug = false;

function coolPhone() {
  var _coolPhone = this;
  this.session = null;
  this.phoneTel = null;
  this.phoneBtn = null;
  this.remoteStream = null;
  this.answeredTone = null;
  this.ringingTone = null;
  this.hangupTone = null;
  this.addr = null;
  var userAgent = null;
  var _Ring = {
    answered: "sounds/answered.wav",
    ringing: "sounds/ringing.wav",
    hangup: "sounds/hangup.wav"
  }

  this.register = function (_socket, _addr, _user, _passwd, _phoneTel, _phoneBtn, _remoteStream, _answeredTone, _ringgingAudio, _hangupTone) {
    this.remoteStream = _remoteStream;
    this.answeredTone = new Audio(_Ring.answered);
    this.ringingTone = new Audio(_Ring.ringing);
    this.ringingTone.loop = true;
    this.hangupTone = new Audio(_Ring.hangup);
    this.phoneTel = _phoneTel;
    this.phoneBtn = _phoneBtn;
    this.addr = _addr;
    var socket = new JsSIP.WebSocketInterface(_socket);
    var uri = "sip:" + _user + "@" + _addr;
    var configuration = {
      sockets: [socket],
      uri: uri,
      authorization_user: _user,
      display_name: _user,
      contact_uri: uri + ';transport=wss',
      password: _passwd,
      register_expires: 300 * 60,
      register: true,
      session_timers: false
    };
    userAgent = new JsSIP.UA(configuration);
    userAgent.start();

    userAgent.on("registered", function (data) {
      _coolPhone.log("registered");
    });

    userAgent.on("unregistered", function (data) {
      _coolPhone.log("unregistered");
    });

    userAgent.on("connecting", function (data) {
      _coolPhone.log("connecting");
    });

    userAgent.on("connected", function (data) {
      _coolPhone.log("connected");
    });

    userAgent.on("disconnected", function (data) {
      _coolPhone.log("disconnected");
      userAgent.start();
    });

    //New incoming or outgoing call event
    userAgent.on("newRTCSession", function (data) {
      _coolPhone.log("newRTCSession " + data.session.direction);
      if (data.session.direction == "incoming") {
        if (_coolPhone.session == null) {
          _coolPhone.session = data.session;
          _coolPhone.bind(_coolPhone.session._request.from.uri._user);
          _coolPhone.session.on("progress", function (data) {
            _coolPhone.log("progress");
            var options = {
              mediaConstraints: {
                audio: true,
                video: false
              },
              sessionTimersExpires: 120,
              pcConfig: {
                'iceServers': [
                  { 'urls': ['stun:' + this.addr, 'stun:stun.psycall.cn:63478'] }
                ]
              }
            };
            _coolPhone.session.answer(options);
            //_coolPhone.playRing(_Ring.ringing);
          });
          _coolPhone.session.on("sdp", function (data) {
            _coolPhone.log("sdp");
          });
          _coolPhone.session.on("confirmed", function (data) {
            _coolPhone.log("confirmed");
            var stream = new MediaStream();
            var receivers = _coolPhone.session.connection.getReceivers();
            if (receivers) receivers.forEach((receiver) => stream.addTrack(receiver.track));
            _coolPhone.remoteStream.srcObject = stream;
            _coolPhone.remoteStream.play();
            _coolPhone.playRing(_Ring.answered);
          });
          _coolPhone.session.on("failed", function (data) {
            _coolPhone.log("failed");
            _coolPhone.playRing(_Ring.hangup);
          });
          _coolPhone.session.on("ended", function (data) {
            _coolPhone.log("ended");
            _coolPhone.playRing(_Ring.hangup);
          });
          _coolPhone.session.on("icecandidate", function (event) {
            _coolPhone.log("icecandidate");
            if (event.candidate.type === "srflx" && event.candidate.relatedAddress !== null && event.candidate.relatedPort !== null) {
              event.ready();
            }
          });
        } else {
          data.session.terminate();
        }
      } else {
        _coolPhone.session = data.session;
      }
    });
  };

  this.makeCall = function (dest) {
    _coolPhone.bind(dest);
    var eventHandlers = {
      progress: function (data) {
        _coolPhone.log("progress");
        if (data.response.status_code == 183) {
          var stream = new MediaStream();
          var receivers = _coolPhone.session.connection.getReceivers();
          if (receivers) receivers.forEach((receiver) => stream.addTrack(receiver.track));
          _coolPhone.remoteStream.srcObject = stream;
          _coolPhone.remoteStream.play();
        }
        else
          _coolPhone.playRing(_Ring.ringing);
      },
      sdp: function (data) {
        _coolPhone.log("sdp");
      },
      confirmed: function (data) {
        _coolPhone.log("confirmed");
        var stream = new MediaStream();
        var receivers = _coolPhone.session.connection.getReceivers();
        if (receivers) receivers.forEach((receiver) => stream.addTrack(receiver.track));
        _coolPhone.remoteStream.srcObject = stream;
        _coolPhone.remoteStream.play();
        _coolPhone.playRing(_Ring.answered);
      },
      failed: function (data) {
        _coolPhone.log("failed");
        _coolPhone.playRing(_Ring.hangup);
      },
      ended: function (data) {
        _coolPhone.log("ended");
        _coolPhone.playRing(_Ring.hangup);
      },
      icecandidate: function (event) {
        _coolPhone.log("icecandidate");
        if (event.candidate.type === "srflx" && event.candidate.relatedAddress !== null && event.candidate.relatedPort !== null) {
          event.ready();
        }
      }
    };
    var options = {
      eventHandlers: eventHandlers,
      mediaConstraints: {
        audio: true,
        video: false
      },
      sessionTimersExpires: 120,
      pcConfig: {
        'iceServers': [
          { 'urls': ['stun:' + this.addr, 'stun:stun.psycall.cn:63478'] }
        ]
      }
    };
    userAgent.call("sip:" + dest + "@" + this.addr, options);
  };

  this.terminateSession = function () {
    if (_coolPhone.session != null)
      _coolPhone.session.terminate();
  }

  this.playRing = async function (file) {
    _coolPhone.log(file);
    this.answeredTone.pause();
    this.hangupTone.pause();
    this.ringingTone.pause();
    if (file == _Ring.answered) {
      this.answeredTone.play().then(() => { }).catch((err) => { });
    } else if (file == _Ring.hangup) {
      _coolPhone.session = null;
      this.bind('');
      this.hangupTone.play().then(() => { }).catch((err) => { });
    } else if (file == _Ring.ringing) {
      this.ringingTone.play().then(() => { }).catch((err) => { });
    }
  };

  this.bind = function (number) {
    this.phoneTel.value = number;
    if (number == '') {
      //this.phoneBtn.innerHTML = "呼叫";
      this.phoneBtn.style.backgroundColor = "#009688";
    }
    else {
      //this.phoneBtn.innerHTML = "挂断";
      this.phoneBtn.style.backgroundColor = "#FF5722";
    }
  };

  this.stop = function () {
    userAgent.stop();
  };

  this.action = function () {
    if (_coolPhone.session == null) {
      var number = this.phoneTel.value.trim();
      if (number == '')
        alert("号码不能为空！");
      else
        this.makeCall(number);
    } else {
      _coolPhone.session.terminate();
    }
    return false;
  }

  this.log = function (info) {
    if (debug == true)
      console.log(info);
  }
}