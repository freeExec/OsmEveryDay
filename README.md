OpenStreetMap Каждый день
=========================

Предназначена для анализа дневных диффов, с целью поиска пользователей, редактирующих OSM каждый день.

## Использование

* Скачать с [Planet](http://planet.osm.org/replication/day) нужную последовательность дней.
* Обработать каждый день

  `OsmEveryDay.exe 573.osc.gz`
  или
  `OsmEveryDay.exe 573.osc`
* Проанализировать полученные промежуточные итоги

  `OsmEveryDay.exe /analize .`
  или
  `OsmEveryDay.exe /analize <каталог-где-лежат-файлы-полученные-на-предыдущем-шаге>`

## Результат
> 573.osc-578.osc.csv

| uid | user | changesets_count | chain_days |
| --- | --- | :---: | :---: |
| 499800 | freeExec | 27 | 5 |