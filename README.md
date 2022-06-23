#金额记录器文档说明
##包、类说明
![图片名称](static/package_introduction.png)  
##启动指导
###开发工具中启动
使用idea打开项目，配置好maven、jdk后，导入项目jar包依赖之后，进入`com.hsbc.controller.demo.Hello`运行main方法即可启动
###脚本文件启动
如果本地已经配置好jdk、maven环境变量，则使用
`mvn package`
命令即可打包编译程序，然后根据图片指引到target目录下，找到启动文件

路径为：target/payment-recoder/bin

其中 `.sh`结尾的是linux启动文件，`.bat`结尾的是windows启动文件

在windows下如果jdk路径已配置到环境变量，则双击启动就能使用

##使用指导

###无论您是通过idea启动还是脚本文件启动，都可以在控制台根据指引操作

![图片名称](static/ideaconsole.png)  

如：`USD 1000` `HKD -100` `CNY 999`
###也可以访问接口操作
获取所有的货币金额：`http://localhost:5000/recoder/all`

根据货币获取金额：`http://localhost:5000/recoder/HKD`

根据货币 加 金额：`http://localhost:5000/recoder/HKD/plus/1`

根据货币 减 金额：`http://localhost:5000/recoder/HKD/reduce/1`



