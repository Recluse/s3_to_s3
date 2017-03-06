## s3\_to\_s3.py

### Предназначение
Скрипт для переноса объектов из одного бакета s3 в другой (всех или для определенного префикса).

### Установка
Рекомендуемая версия python >= 3.4
Желательно окружение virtualenv

 1. `virtualenv env`
 - `. env/bin/activate`
 - `pip install -r requirements.txt`
 - `python s3_to_s3.py --help`

При отсутствии `virtualenv` в системе, достаточно пунктов 3 и 4 (команду `pip` следует заменить на `pip3`)

### Использование

Для использования необходим конфигурационный файл, пример есть в репо проекта. Необходимо
изменить значения в секциях Source и Destination на нужные.
Для использования другого конфигурационного файла укажите опцию `-c /path/to/conf`


`python s3_to_s3.py --help`

```
usage: s3_to_s3.py [-h] [-m MARKER] [-t THREADS] [-r] [-i MAX_ITEMS] [-s]
                   [--debug]
                   [prefix]

positional arguments:
  prefix

optional arguments:
  -h, --help            show this help message and exit
  -m MARKER, --marker MARKER
                        start marker of paged s3 iterator
  -c CONFIG, --config CONFIG
                        config file for use
  -t THREADS, --threads THREADS
                        number of threads
  -r, --restore         restore marker from file
  -i MAX_ITEMS, --max-items MAX_ITEMS
                        max items to process
  -s, --safe-mode       save marker position with each iteration
  --debug               enable debug output
```

`prefix` - позволяет фильтровать имеющиеся ключи по префиксу. Например из ключей
    [
        'one/two/1',
        'one/two/2',
        'two/three/1'
    ]
при выбранном префиксе `one` будут выбраны `['one/two/1','one/two/2']`

### Marker и восстановление
Маркер - это запомненная позиция итератора всех выбранных объектов. Если скрипт аварийно завершить (например послав ему ctrl+c)
маркер будет сохранен в конфигурационный файл. При запуске с ключем `-r` скрипт продолжит работу с того места, на котором остановился.

Для тех, кто боится аварийного отключения питания и\или deadlock в скрипте, существует флаг `-s`, он в свою очередь записывает маркер
в конфигурационный файл каждый раз. **По умолчанию флаг выключен**

### Логирование
Путь до файла настраивается в конфигурационном файле, в секции `Global`, уровень детализации - ключом `--debug`.