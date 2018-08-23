<?php

namespace HuangYi\FlysystemAliyunOss;

use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\CanOverwriteFiles;
use League\Flysystem\Adapter\Polyfill\StreamedTrait;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use OSS\OssClient;

class AliyunOssAdapter extends AbstractAdapter implements CanOverwriteFiles
{
    use StreamedTrait;

    /**
     * OssClient.
     *
     * @var \OSS\OssClient
     */
    protected $client;

    /**
     * Bucket.
     *
     * @var string
     */
    protected $bucket;

    /**
     * Options.
     *
     * @var array
     */
    protected $options;

    /**
     * Aliyun OSS Adapter.
     *
     * @param \OSS\OssClient $client
     * @param string $bucket
     * @param string $prefix
     * @param array $options
     * @return void
     */
    public function __construct(OssClient $client, $bucket, $prefix = null, $options = null)
    {
        $this->setClient($client);
        $this->setBucket($bucket);
        $this->setPathPrefix($prefix);
        $this->setOptions($options);
    }

    /**
     * Check whether a file exists.
     *
     * @param string $path
     *
     * @return bool
     */
    public function has($path)
    {
        $path = $this->applyPathPrefix($path);

        return $this->client->doesObjectExist($this->bucket, $path, $this->options);
    }

    /**
     * Read a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function read($path)
    {
        $path = $this->applyPathPrefix($path);
        $contents = $this->client->getObject($this->bucket, $path, $this->options);

        return compact('contents');
    }

    /**
     * List contents of a directory.
     *
     * @param string $directory
     * @param bool $recursive
     *
     * @return array
     *
     * @throws \OSS\Core\OssException
     */
    public function listContents($directory = '', $recursive = false)
    {
        $options = [
            'max-keys' => 1000,
            'prefix' => $this->applyPathPrefix($directory),
        ];

        if (! $recursive) {
            $options['delimiter'] = '/';
        }

        $marker = '';
        $contents = [];

        do {
            $options['marker'] = $marker;

            $results = $this->client->listObjects($this->bucket, $options);

            foreach ($results->getObjectList() as $object) {
                $contents[] = [
                    'type' => $object->getSize() === 0 ? 'dir' : 'file',
                    'path' => $this->removePathPrefix($object->getKey()),
                    'timestamp' => strtotime($object->getLastModified()),
                    'size' => $object->getSize(),
                ];
            }

            foreach ($results->getPrefixList() as $object) {
                $contents[] = [
                    'type' => 'dir',
                    'path' => $this->removePathPrefix($object->getPrefix()),
                    'timestamp' => 0,
                ];
            }
        } while ($marker = $results->getNextMarker());

        return $contents;
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getMetadata($path)
    {
        $path = $this->applyPathPrefix($path);

        return $this->client->getObjectMeta($this->bucket, $path, $this->options);
    }

    /**
     * Get the size of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getSize($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * Get the mimetype of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getMimetype($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * Get the last modified time of a file as a timestamp.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * Write a new file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config   Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function write($path, $contents, Config $config)
    {
        $pathWithPrefix = $this->applyPathPrefix($path);

        $this->client->putObject($this->bucket, $pathWithPrefix, $contents, $this->options);

        return ['path' => $path];
    }

    /**
     * Update a file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config   Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function update($path, $contents, Config $config)
    {
        return $this->write($path, $contents, $config);
    }

    /**
     * Rename a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function rename($path, $newpath)
    {
        $this->copy($path, $newpath);
        $this->delete($path);

        return true;
    }

    /**
     * Copy a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     *
     * @throws \OSS\Core\OssException
     */
    public function copy($path, $newpath)
    {
        $path = $this->applyPathPrefix($path);
        $newpath = $this->applyPathPrefix($newpath);

        $this->client->copyObject($this->bucket, $path, $this->bucket, $newpath, $this->options);

        return true;
    }

    /**
     * Delete a file.
     *
     * @param string $path
     *
     * @return bool
     */
    public function delete($path)
    {
        $path = $this->applyPathPrefix($path);

        $this->client->deleteObject($this->bucket, $path, $this->options);

        return true;
    }

    /**
     * Delete a directory.
     *
     * @param string $dirname
     *
     * @return bool
     */
    public function deleteDir($dirname)
    {
        return $this->delete($dirname);
    }

    /**
     * Create a directory.
     *
     * @param string $dirname directory name
     * @param Config $config
     *
     * @return array|false
     */
    public function createDir($dirname, Config $config)
    {
        $dirname = rtrim($this->applyPathPrefix($dirname), '/');

        $this->client->createObjectDir($this->bucket, $dirname, $this->options);

        return ['path' => $dirname, 'type' => 'dir'];
    }

    /**
     * Set the visibility for a file.
     *
     * @param string $path
     * @param string $visibility
     *
     * @return array|false
     *
     * @throws \OSS\Core\OssException
     */
    public function setVisibility($path, $visibility)
    {
        $pathWithPrefix = $this->applyPathPrefix($path);

        $acl = ($visibility === AdapterInterface::VISIBILITY_PUBLIC ) ? 'public-read' : 'private';

        $this->client->putObjectAcl($this->bucket, $pathWithPrefix, $acl);

        return ['visibility' => $visibility];
    }

    /**
     * Get the visibility of a file.
     *
     * @param string $path
     *
     * @return array|false
     *
     * @throws \OSS\Core\OssException
     */
    public function getVisibility($path)
    {
        $path = $this->applyPathPrefix($path);

        $acl = $this->client->getObjectAcl($this->bucket, $path);

        $visibility = $acl === 'private' ? AdapterInterface::VISIBILITY_PRIVATE : AdapterInterface::VISIBILITY_PUBLIC;

        return ['visibility' => $visibility];
    }

    /**
     * Get bucket.
     *
     * @return string
     */
    public function getBucket()
    {
        return $this->bucket;
    }

    /**
     * Set bucket.
     *
     * @param string $bucket
     * @return $this
     */
    public function setBucket($bucket)
    {
        $this->bucket = $bucket;

        return $this;
    }

    /**
     * Get options.
     *
     * @return array
     */
    public function getOptions()
    {
        return $this->options;
    }

    /**
     * Set options.
     *
     * @param array $options
     * @return $this
     */
    public function setOptions($options)
    {
        $this->options = $options;

        return $this;
    }

    /**
     * Get OssClient.
     *
     * @return \OSS\OssClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Set OssClient.
     *
     * @param \OSS\OssClient $client
     * @return $this
     */
    public function setClient(OssClient $client)
    {
        $this->client = $client;

        return $this;
    }
}
