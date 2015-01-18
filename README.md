collectWeiboData
================
## 收集含关键词的新浪微博数据

**利用微博高级搜索功能，按关键字搜集一定时间范围内的微博。**详见[博客](http://blog.csdn.net/heloowird/article/details/38149451)

1. 大体思路：构造URL，爬取网页，然后解析网页中的微博ID。后续利用微博API进行数据入库。本程序只负责收集微博的ID。
>登陆新浪微博，进入高级搜索，输入关键字”空气污染“，选择”实时“，时间为”2013-07-02-2:2013-07-09-2“，地区为”北京“，之后发送请求会发现地址栏变为如下:
>   http://s.weibo.com/wb/%25E7%25A9%25BA%25E6%25B0%2594%25E6%25B1%25A1%25E6%259F%2593&xsort=time&region=custom:11:1000&timescope=custom:2013-07-02-2:2013-07-09-2&Refer=g
> > + 固定地址部分：http://s.weibo.com/wb/
> > + 关键字二次UTF-8编码：%25E7%25A9%25BA%25E6%25B0%2594%25E6%25B1%25A1%25E6%259F%2593
> > + 排序为“实时”：xsort=time
> > + 搜索地区：region=custom:11:1000
> > + 搜索时间范围：timescope=custom:2013-07-02-2:2013-07-09-2
> > + 可忽略项：Refer=g
> > + 显示类似微博：nodup=1    注：这个选项可多收集微博，建议加上。默认不加此参数，省略了部分相似微博。
> > + 某次请求的页数：page=1
> * 另外，高级搜索最多返回50页微博，那么时间间隔设置最小为宜。所以该类设置为搜集一定时间段内最多50页微博*

2. 依赖包：lxml(解析网页)、py2exe(编译成windows窗口程序依赖包)

3. 编译方法：windows进入控制台，运行python setup.py py2exe，即可生成window窗口程序

## 收集含GPS的新浪微博数据

**利用微博API，按一定空间范围搜集一定时间范围内的含GPS微博。**详见[博客](http://blog.csdn.net/heloowird/article/details/40626375)

1. 大体思路：
> 1. 选择多个中心点，以10km为半径做buffer覆盖整个城市；
> 2. 圆形区域较多，可采用多线程进行。一个buffer对应一个圆形区域，对应一个线程；
> 3. 第三步：用额外的线程将采集到的微博数据入库。

2. 依赖包：yaml(搜集参数)、pymongo(连接数据库MongoDB)、py2exe(编译成windows窗口程序依赖包)

3. 编译方法：windows进入控制台，运行python setup.py py2exe，即可生成window窗口程序

4. 配置文件：详见config.yaml文件
