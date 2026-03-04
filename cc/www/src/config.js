
layui.define(['laytpl', 'layer', 'element', 'util'], function(exports){
  var table = layui.table;
	var name = window.sessionStorage.getItem("name");
  var access_token2 = "";
  if (layui.data('layuiAdmin')[name] != undefined) {
    access_token2 = layui.data('layuiAdmin')[name]["access_token"]
  }
  table.set({
    headers: { //通过 request 头传递
      access_token: access_token2
    },
    ContentType: 'application/x-www-form-urlencoded; charset=UTF-8'
  });
  exports('setter', {
    container: 'LAY_app'
    ,base: layui.cache.base
    ,views: layui.cache.base + 'views/'
    ,entry: 'index'
    ,engine: '.html'
    ,pageTabs: false
    
    ,name: 'PSCC'
    ,tableName: 'layuiAdmin'
    ,MOD_NAME: 'admin'
    
    ,debug: true
    
    ,interceptor: true
    
    ,request: {
      tokenName: 'access_token'
    }
    

    ,response: {
      statusName: 'code'
      ,statusCode: {
        ok: 0
        ,logout: 1001
      }
      ,msgName: 'msg' 
      ,dataName: 'data'
    }
    

    ,indPage: [
      '/user/login'
      ,'/user/reg'
      ,'/user/forget'
      ,'/template/tips/test'
    ]
    

    ,extend: {
      echarts: 'echarts',
      echartsTheme: 'echartsTheme',
      layim: 'layim/layim'
    }
    ,configUrl: '/../'
    ,theme: {
      color: [{
        main: '#20222A'
        ,selected: '#009688'
        ,alias: 'default'
      },{
        main: '#03152A'
        ,selected: '#3B91FF'
        ,alias: 'dark-blue'
      },{
        main: '#2E241B'
        ,selected: '#A48566'
        ,alias: 'coffee'
      },{
        main: '#50314F'
        ,selected: '#7A4D7B'
        ,alias: 'purple-red'
      },{
        main: '#344058'
        ,logo: '#1E9FFF'
        ,selected: '#1E9FFF'
        ,alias: 'ocean'
      },{
        main: '#3A3D49'
        ,logo: '#2F9688'
        ,selected: '#5FB878'
        ,alias: 'green'
      },{
        main: '#20222A'
        ,logo: '#F78400'
        ,selected: '#F78400'
        ,alias: 'red'
      },{
        main: '#28333E'
        ,logo: '#AA3130'
        ,selected: '#AA3130'
        ,alias: 'fashion-red'
      },{
        main: '#24262F'
        ,logo: '#3A3D49'
        ,selected: '#009688'
        ,alias: 'classic-black'
      },{
        logo: '#226A62'
        ,header: '#2F9688'
        ,alias: 'green-header'
      },{
        main: '#344058'
        ,logo: '#0085E8'
        ,selected: '#1E9FFF'
        ,header: '#1E9FFF'
        ,alias: 'ocean-header'
      },{
        header: '#393D49'
        ,alias: 'classic-black-header'
      },{
        main: '#50314F'
        ,logo: '#50314F'
        ,selected: '#7A4D7B'
        ,header: '#50314F'
        ,alias: 'purple-red-header'
      },{
        main: '#28333E'
        ,logo: '#28333E'
        ,selected: '#AA3130'
        ,header: '#AA3130'
        ,alias: 'fashion-red-header'
      },{
        main: '#28333E'
        ,logo: '#009688'
        ,selected: '#009688'
        ,header: '#009688'
        ,alias: 'green-header'
      },{
        main: '#393D49'
        ,logo: '#393D49'
        ,selected: '#009688'
        ,header: '#23262E'
        ,alias: 'Classic-style1'
      },{
        main: '#001529'
        ,logo: '#001529'
        ,selected: '#1890FF'
        ,header: '#1890FF'
        ,alias: 'Classic-style2'
      },{
        main: '#25282A'
        ,logo: '#25282A'
        ,selected: '#35BDB2'
        ,header: '#35BDB2'
        ,alias: 'Classic-style3'
      }]
      ,initColorIndex: 10
    }
  });
});
