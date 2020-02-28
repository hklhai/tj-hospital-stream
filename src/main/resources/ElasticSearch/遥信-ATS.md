## 遥测 ATS
``` 
PUT yc_ats
PUT /yc_ats/_mapping/ats
{
    "properties": {
        "IEDName": {
            "type": "text"
        },
        "UA": {
            "type": "double"
        },
        "UB": {
            "type": "double"
        },
        "UC": {
            "type": "double"
        },
        "IA": {
            "type": "double"
        },
        "IB": {
            "type": "double"
        },
        "IC": {
            "type": "double"
        },
        "ColTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
        },
        "CreateTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
        },
        "assetYpe": {
            "type": "text"
        },
        "productModel": {
            "type": "text"
        },
        "assetYpe1": {
            "type": "text"
        },
        "assetYpe2": {
            "type": "text"
        }
    }
}
```


## 遥信ATS
```
PUT yx_ats
PUT /yx_ats/_mapping/ats
{
    "properties": {
        "IEDName": {
            "type": "text"
        },
        "VariableName": {
            "type": "text"
        },
        "Value": {
            "type": "integer"
        },
        "ColTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
        },
        "CreateTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
        },
        "assetYpe": {
            "type": "text"
        },
        "productModel": {
            "type": "text"
        },
        "assetYpe1": {
            "type": "text"
        },
        "assetYpe2": {
            "type": "text"
        }
    }
}
```