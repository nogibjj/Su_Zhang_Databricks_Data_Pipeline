```SQL

    SELECT 
        country,
        beer_servings,
        wine_servings,
        spirit_servings,
        beer_percentage,
        CASE 
            WHEN beer_percentage > 0.5 THEN 'High Beer Consumption'
            ELSE 'Moderate/Low Beer Consumption'
        END AS beer_consumption_category
    FROM ids706_data_engineering.default.sz324_drinks_delta_transformed
    WHERE beer_percentage IS NOT NULL
    ORDER BY beer_percentage DESC
    LIMIT 20;
    
```

```Query received, executing query.
```

```SQL

    SELECT 
        country,
        beer_servings,
        wine_servings,
        spirit_servings,
        beer_percentage,
        CASE 
            WHEN beer_percentage > 0.5 THEN 'High Beer Consumption'
            ELSE 'Moderate/Low Beer Consumption'
        END AS beer_consumption_category
    FROM ids706_data_engineering.default.sz324_drinks_delta_transformed
    WHERE beer_percentage IS NOT NULL
    ORDER BY beer_percentage DESC
    LIMIT 20;
    
```

```Query received, executing query.
```

```SQL

    SELECT 
        country,
        beer_servings,
        wine_servings,
        spirit_servings,
        beer_percentage,
        CASE 
            WHEN beer_percentage > 0.5 THEN 'High Beer Consumption'
            ELSE 'Moderate/Low Beer Consumption'
        END AS beer_consumption_category
    FROM ids706_data_engineering.default.sz324_drinks_delta_transformed
    WHERE beer_percentage IS NOT NULL
    ORDER BY beer_percentage DESC
    LIMIT 20;
    
```

```| country                  |   beer_servings |   wine_servings |   spirit_servings |   beer_percentage | beer_consumption_category   |
|:-------------------------|----------------:|----------------:|------------------:|------------------:|:----------------------------|
| Yemen                    |               6 |               0 |                 0 |          1        | High Beer Consumption       |
| Burundi                  |              88 |               0 |                 0 |          1        | High Beer Consumption       |
| Eritrea                  |              18 |               0 |                 0 |          1        | High Beer Consumption       |
| Bhutan                   |              23 |               0 |                 0 |          1        | High Beer Consumption       |
| Namibia                  |             376 |               1 |                 3 |          0.989474 | High Beer Consumption       |
| Vietnam                  |             111 |               1 |                 2 |          0.973684 | High Beer Consumption       |
| Cameroon                 |             147 |               4 |                 1 |          0.967105 | High Beer Consumption       |
| Swaziland                |              90 |               2 |                 2 |          0.957447 | High Beer Consumption       |
| Rwanda                   |              43 |               0 |                 2 |          0.955556 | High Beer Consumption       |
| Brunei                   |              31 |               1 |                 2 |          0.911765 | High Beer Consumption       |
| DR Congo                 |              32 |               1 |                 3 |          0.888889 | High Beer Consumption       |
| Gambia                   |               8 |               1 |                 0 |          0.888889 | High Beer Consumption       |
| Congo                    |              76 |               9 |                 1 |          0.883721 | High Beer Consumption       |
| Chad                     |              15 |               1 |                 1 |          0.882353 | High Beer Consumption       |
| Ethiopia                 |              20 |               0 |                 3 |          0.869565 | High Beer Consumption       |
| Nauru                    |              49 |               8 |                 0 |          0.859649 | High Beer Consumption       |
| Nigeria                  |              42 |               2 |                 5 |          0.857143 | High Beer Consumption       |
| Central African Republic |              17 |               1 |                 2 |          0.85     | High Beer Consumption       |
| South Korea              |             140 |               9 |                16 |          0.848485 | High Beer Consumption       |
| Tanzania                 |              36 |               1 |                 6 |          0.837209 | High Beer Consumption       |
```

```SQL

    SELECT 
        country,
        beer_servings,
        wine_servings,
        spirit_servings,
        beer_percentage,
        CASE 
            WHEN beer_percentage > 0.5 THEN 'High Beer Consumption'
            ELSE 'Moderate/Low Beer Consumption'
        END AS beer_consumption_category
    FROM ids706_data_engineering.default.sz324_drinks_delta_transformed
    WHERE beer_percentage IS NOT NULL
    ORDER BY beer_percentage DESC
    LIMIT 20;
    
```

```Query received, executing query.
```

```SQL

    SELECT 
        country,
        beer_servings,
        wine_servings,
        spirit_servings,
        beer_percentage,
        CASE 
            WHEN beer_percentage > 0.5 THEN 'High Beer Consumption'
            ELSE 'Moderate/Low Beer Consumption'
        END AS beer_consumption_category
    FROM ids706_data_engineering.default.sz324_drinks_delta_transformed
    WHERE beer_percentage IS NOT NULL
    ORDER BY beer_percentage DESC
    LIMIT 20;
    
```

```| country                  |   beer_servings |   wine_servings |   spirit_servings |   beer_percentage | beer_consumption_category   |
|:-------------------------|----------------:|----------------:|------------------:|------------------:|:----------------------------|
| Bhutan                   |              23 |               0 |                 0 |             1     | High Beer Consumption       |
| Burundi                  |              88 |               0 |                 0 |             1     | High Beer Consumption       |
| Eritrea                  |              18 |               0 |                 0 |             1     | High Beer Consumption       |
| Yemen                    |               6 |               0 |                 0 |             1     | High Beer Consumption       |
| Namibia                  |             376 |               1 |                 3 |             0.989 | High Beer Consumption       |
| Vietnam                  |             111 |               1 |                 2 |             0.974 | High Beer Consumption       |
| Cameroon                 |             147 |               4 |                 1 |             0.967 | High Beer Consumption       |
| Swaziland                |              90 |               2 |                 2 |             0.957 | High Beer Consumption       |
| Rwanda                   |              43 |               0 |                 2 |             0.956 | High Beer Consumption       |
| Brunei                   |              31 |               1 |                 2 |             0.912 | High Beer Consumption       |
| DR Congo                 |              32 |               1 |                 3 |             0.889 | High Beer Consumption       |
| Gambia                   |               8 |               1 |                 0 |             0.889 | High Beer Consumption       |
| Congo                    |              76 |               9 |                 1 |             0.884 | High Beer Consumption       |
| Chad                     |              15 |               1 |                 1 |             0.882 | High Beer Consumption       |
| Ethiopia                 |              20 |               0 |                 3 |             0.87  | High Beer Consumption       |
| Nauru                    |              49 |               8 |                 0 |             0.86  | High Beer Consumption       |
| Nigeria                  |              42 |               2 |                 5 |             0.857 | High Beer Consumption       |
| Central African Republic |              17 |               1 |                 2 |             0.85  | High Beer Consumption       |
| South Korea              |             140 |               9 |                16 |             0.848 | High Beer Consumption       |
| Tanzania                 |              36 |               1 |                 6 |             0.837 | High Beer Consumption       |
```

