
 
layui.extend({
  setter: 'config'
  ,admin: 'lib/admin'
  ,view: 'lib/view'
}).define(['setter', 'admin'], function(exports){
  var setter = layui.setter
  ,element = layui.element
  ,admin = layui.admin
  ,tabsPage = admin.tabsPage
  ,view = layui.view
  
  ,renderPage = function(){
    var router = layui.router()
    ,path = router.path
    ,pathURL = admin.correctRouter(router.path.join('/'))

    
    
    if(!path.length) path = [''];
    
    if(path[path.length - 1] === ''){
      path[path.length - 1] = setter.entry;
    }
    
    
    var reset = function(type){
      if(renderPage.haveInit){
        $('.layui-layer').each(function(){
          var othis = $(this),
          index = othis.attr('times');
          if(!othis.hasClass('layui-layim')){
            layer.close(index);
          }
        });
      }
      renderPage.haveInit = true;
      
      $(APP_BODY).scrollTop(0);
      delete tabsPage.type;
    };
    
    if(tabsPage.type === 'tab'){
      if(pathURL !== '/' || (pathURL === '/' && admin.tabsBody().html())){
        admin.tabsBodyChange(tabsPage.index);
        return reset(tabsPage.type);
      }
    }
    
    view().render(path.join('/')).then(function(res){
      
      var matchTo
      ,tabs = $('#LAY_app_tabsheader>li');
      
      tabs.each(function(index){
        var li = $(this)
        ,layid = li.attr('lay-id');
        
        if(layid === pathURL){
          matchTo = true;
          tabsPage.index = index;
        }
      });
      
      if(setter.pageTabs && pathURL !== '/'){
        if(!matchTo){
          $(APP_BODY).append('<div class="layadmin-tabsbody-item layui-show"></div>');
          tabsPage.index = tabs.length;
          element.tabAdd(FILTER_TAB_TBAS, {
            title: '<span>'+ (res.title || '新标签页') +'</span>'
            ,id: pathURL
            ,attr: router.href
          });
        }
      }
      
      this.container = admin.tabsBody(tabsPage.index);
      setter.pageTabs || this.container.scrollTop(0);
      
      element.tabChange(FILTER_TAB_TBAS, pathURL);
      admin.tabsBodyChange(tabsPage.index);
      
    }).done(function(){
      layui.use('common', layui.cache.callback.common);
      $win.on('resize', layui.data.resize);
      
      element.render('breadcrumb', 'breadcrumb');
      
      admin.tabsBody(tabsPage.index).on('scroll', function(){
        var othis = $(this)
        ,elemDate = $('.layui-laydate')
        ,layerOpen = $('.layui-layer')[0];

        if(elemDate[0]){
          elemDate.each(function(){
            var thisElemDate = $(this);
            thisElemDate.hasClass('layui-laydate-static') || thisElemDate.remove();
          });
          othis.find('input').blur();
        }
        
        layerOpen && layer.closeAll('tips');
      });
    });
    
    reset();
  }
  
  ,entryPage = function(fn){
    var router = layui.router()
    ,container = view(setter.container)
    ,pathURL = admin.correctRouter(router.path.join('/'))
    ,isIndPage;
    
    layui.each(setter.indPage, function(index, item){
      if(pathURL === item){
        return isIndPage = true;
      }
    });
    
    layui.config({
      base: setter.base + 'controller/'
    });
    
    if(isIndPage || pathURL === '/user/login'){ 
      container.render(router.path.join('/')).done(function(){
        admin.pageType = 'alone';
      });
    } else {
      
      if(setter.interceptor){
        var local = layui.data(setter.tableName);
        if(!local[setter.request.tokenName]){
          return location.hash = '/user/login';
        }
      }
      
      if(admin.pageType === 'console') {
        renderPage();
      } else {
        container.render('layout').done(function(){
          renderPage();
          layui.element.render();
          
          if(admin.screen() < 2){
            admin.sideFlexible();
          }
          admin.pageType = 'console';
        }); 
      }
      
    }
  }
  
  ,APP_BODY = '#LAY_app_body', FILTER_TAB_TBAS = 'layadmin-layout-tabs'
  ,$ = layui.$, $win = $(window);
  
  layui.link(
    setter.base + 'style/admin.css?v='+ (admin.v + '-1')
    ,function(){
      entryPage()
    }
    ,'layuiAdmin'
  );
  
  window.onhashchange = function(){
    entryPage();
    layui.event.call(this, setter.MOD_NAME, 'hash({*})', layui.router());
  };
  
  layui.each(setter.extend, function(key, value){
    var mods = {}
    ,_isArray = setter.extend.constructor === Array;
    mods[_isArray ? value : key] = '{/}' + setter.base + 'lib/extend/' + value;
    layui.extend(mods);
  });

  exports('index', {
    render: renderPage
  });
});
