# Ycrawler
##  Задание: написать асинхронный краулер для новостного сайта news.ycombinator.com:
* краулер начинает обкачивать сайт с корня news.ycombinator.com/
* краулер должен обкачивать топ новостей, т.е. первые 30 на корневой станице, после чего ждать появления новых новостей в топе, которых он раньше не видел
* для того, чтобы "скачать" новость нужно скачать непосредственно страницу на которую она ведет и загрузить все страницы по ссылкам в комментариях к новости
* внутри скачанных страниц далее новые ссылки искать не нужно
* скачанная новость со всеми скачанными страницами из комментариев должна лежать в отдельной папке на диске 
* разрешается использовать стандартную библиотеку и aiohttp.

## Usage
```
# Python 3.6.8
# aiohttp==3.5.4


ycrawler.py [options]

Options:
  -h, --help  show this help message and exit
  --cfg=CFG   Configuration file name, default is ycomb.cfg
```