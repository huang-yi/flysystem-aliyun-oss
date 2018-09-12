# Flysystem-Aliyun-OSS

This package provides a Flysystem adapter for the Aliyun OSS.

## Usage

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use HuangYi\AliyunOss\OssClient;
use HuangYi\FlysystemAliyunOss\AliyunOssAdapter;
use League\Flysystem\Filesystem;

$bucketName = 'bucket';
$endpoint = 'oss-cn-hangzhou.aliyuncs.com';
$accessKeyId = 'access_key_id';
$accessKeySecret = 'access_key_secret';

$client = new OssClient($bucketName, $endpoint, $accessKeyId, $accessKeySecret);
$adapter = new AliyunOssAdapter($client);
$filesystem = new Filesystem($adapter);

```
