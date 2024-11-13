# ETL Process Log

Spark session created

Load data successfully

The truncated output is: 

|    | country           |   beer_servings |   spirit_servings |   wine_servings |   total_litres_of_pure_alcohol |
|---:|:------------------|----------------:|------------------:|----------------:|-------------------------------:|
|  0 | Afghanistan       |               0 |                 0 |               0 |                            nan |
|  1 | Albania           |              89 |               132 |              54 |                            nan |
|  2 | Algeria           |              25 |                 0 |              14 |                            nan |
|  3 | Andorra           |             245 |               138 |             312 |                            nan |
|  4 | Angola            |             217 |                57 |              45 |                            nan |
|  5 | Antigua & Barbuda |             102 |               128 |              45 |                            nan |
|  6 | Argentina         |             193 |                25 |             221 |                            nan |
|  7 | Armenia           |              21 |               179 |              11 |                            nan |
|  8 | Australia         |             261 |                72 |             212 |                            nan |
|  9 | Austria           |             279 |                75 |             191 |                            nan |

Load data successfully

The truncated output is: 

|    | country           |   beer_servings |   spirit_servings |   wine_servings |   total_litres_of_pure_alcohol |
|---:|:------------------|----------------:|------------------:|----------------:|-------------------------------:|
|  0 | Afghanistan       |               0 |                 0 |               0 |                            nan |
|  1 | Albania           |              89 |               132 |              54 |                            nan |
|  2 | Algeria           |              25 |                 0 |              14 |                            nan |
|  3 | Andorra           |             245 |               138 |             312 |                            nan |
|  4 | Angola            |             217 |                57 |              45 |                            nan |
|  5 | Antigua & Barbuda |             102 |               128 |              45 |                            nan |
|  6 | Argentina         |             193 |                25 |             221 |                            nan |
|  7 | Armenia           |              21 |               179 |              11 |                            nan |
|  8 | Australia         |             261 |                72 |             212 |                            nan |
|  9 | Austria           |             279 |                75 |             191 |                            nan |

Query data

The truncated output is: 

|    | country           |   beer_servings |   spirit_servings |   wine_servings |   total_litres_of_pure_alcohol |
|---:|:------------------|----------------:|------------------:|----------------:|-------------------------------:|
|  0 | Andorra           |             245 |               138 |             312 |                            nan |
|  1 | Angola            |             217 |                57 |              45 |                            nan |
|  2 | Antigua & Barbuda |             102 |               128 |              45 |                            nan |
|  3 | Argentina         |             193 |                25 |             221 |                            nan |
|  4 | Australia         |             261 |                72 |             212 |                            nan |
|  5 | Austria           |             279 |                75 |             191 |                            nan |
|  6 | Bahamas           |             122 |               176 |              51 |                            nan |
|  7 | Barbados          |             143 |               173 |              36 |                            nan |
|  8 | Belarus           |             142 |               373 |              42 |                            nan |
|  9 | Belgium           |             295 |                84 |             212 |                            nan |

Load data successfully

The truncated output is: 

|    | country           |   beer_servings |   spirit_servings |   wine_servings |   total_litres_of_pure_alcohol |
|---:|:------------------|----------------:|------------------:|----------------:|-------------------------------:|
|  0 | Afghanistan       |               0 |                 0 |               0 |                            nan |
|  1 | Albania           |              89 |               132 |              54 |                            nan |
|  2 | Algeria           |              25 |                 0 |              14 |                            nan |
|  3 | Andorra           |             245 |               138 |             312 |                            nan |
|  4 | Angola            |             217 |                57 |              45 |                            nan |
|  5 | Antigua & Barbuda |             102 |               128 |              45 |                            nan |
|  6 | Argentina         |             193 |                25 |             221 |                            nan |
|  7 | Armenia           |              21 |               179 |              11 |                            nan |
|  8 | Australia         |             261 |                72 |             212 |                            nan |
|  9 | Austria           |             279 |                75 |             191 |                            nan |

Data transformed: added 'beer_percentage' columns

The truncated output is: 

|    | country           |   beer_servings |   spirit_servings |   wine_servings |   total_litres_of_pure_alcohol |   beer_percentage |
|---:|:------------------|----------------:|------------------:|----------------:|-------------------------------:|------------------:|
|  0 | Afghanistan       |               0 |                 0 |               0 |                            nan |         nan       |
|  1 | Albania           |              89 |               132 |              54 |                            nan |          32.3636  |
|  2 | Algeria           |              25 |                 0 |              14 |                            nan |          64.1026  |
|  3 | Andorra           |             245 |               138 |             312 |                            nan |          35.2518  |
|  4 | Angola            |             217 |                57 |              45 |                            nan |          68.0251  |
|  5 | Antigua & Barbuda |             102 |               128 |              45 |                            nan |          37.0909  |
|  6 | Argentina         |             193 |                25 |             221 |                            nan |          43.9636  |
|  7 | Armenia           |              21 |               179 |              11 |                            nan |           9.95261 |
|  8 | Australia         |             261 |                72 |             212 |                            nan |          47.8899  |
|  9 | Austria           |             279 |                75 |             191 |                            nan |          51.1927  |
| 10 | Azerbaijan        |              21 |                46 |               5 |                            nan |          29.1667  |
| 11 | Bahamas           |             122 |               176 |              51 |                            nan |          34.957   |
| 12 | Bahrain           |              42 |                63 |               7 |                            nan |          37.5     |
| 13 | Bangladesh        |               0 |                 0 |               0 |                            nan |         nan       |
| 14 | Barbados          |             143 |               173 |              36 |                            nan |          40.625   |

