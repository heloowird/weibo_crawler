# coding: utf-8
from distutils.core import setup
import py2exe

## 进入控制台，输入python setup.py py2exe，即可生成window窗口程序
setup(
    console=[{'script': 'collectWeiboDataByKeyword.py'}],
    options={
        'py2exe': 
        {
            'includes': ['lxml.etree', 'lxml._elementpath', 'gzip'],
        }
    }
)
