``` 
PUT tjerr
PUT /tjerr/_mapping/err
{
    "properties": {
        "Content": {
            "type": "text"
        },
        "CreateTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
        } 
    }
}
```

 