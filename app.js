const path = require('path');

module.exports = app => {
    const servicePaths = app.loader.getLoadUnits().map(unit => path.join(unit.path, 'app/dao'));

    app.loader.loadToContext(servicePaths, 'dao', {
        // dao 需要继承 app.Dao app 参数
        // 设置 call 在加载时会调用函数返回 UserDao
        call: true,
        // 将文件加载到 app.daoClasses
        fieldClass: 'daoClasses',
    });
};