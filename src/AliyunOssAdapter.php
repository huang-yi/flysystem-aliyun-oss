<?php

namespace HuangYi\FlysystemAliyunOss;

use Closure;
use HuangYi\AliyunOss\Contracts\ResponseContract;
use HuangYi\AliyunOss\Exceptions\RequestException;
use HuangYi\AliyunOss\OssClient;
use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\CanOverwriteFiles;
use League\Flysystem\Adapter\Polyfill\StreamedTrait;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;

class AliyunOssAdapter extends AbstractAdapter implements CanOverwriteFiles
{
    use StreamedTrait;

    /**
     * OssClient.
     *
     * @var \HuangYi\AliyunOss\OssClient
     */
    protected $client;

    /**
     * Options.
     *
     * @var array
     */
    protected $options;

    /**
     * Aliyun OSS Adapter.
     *
     * @param \HuangYi\AliyunOss\OssClient $client
     * @param string $prefix
     * @param array $options
     * @return void
     */
    public function __construct(OssClient $client, string $prefix = '', array $options = [])
    {
        $this->setClient($client);
        $this->setPathPrefix($prefix);
        $this->setOptions($options);
    }

    /**
     * Check whether a file exists.
     *
     * @param string $path
     * @return bool
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function has($path)
    {
        $path = $this->applyPathPrefix($path);

        return $this->handleRequest(function () use ($path) {
            $this->client->object->headObject($path);

            return true;
        });
    }

    /**
     * Read a file.
     *
     * @param string $path
     * @return array|false
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function read($path)
    {
        $path = $this->applyPathPrefix($path);

        return $this->handleRequest(function () use ($path) {
            $response = $this->client->object->getObject($path, $this->options);

            return $this->transformFile($response, $path);
        });
    }

    /**
     * List contents of a directory.
     *
     * @param string $directory
     * @param bool $recursive
     * @return array
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function listContents($directory = '', $recursive = false)
    {
        $query = $this->getListContentsQuery($directory, $recursive);

        $contents = [];

        do {
            if (isset($nextMarker)) {
                $query['marker'] = $nextMarker;
            }

            $response = $this->client->bucket->listObjects(['query' => $query]);
            $body = $response->getBody();

            if (isset($body['Contents'])) {
                $contents = array_merge(
                    $contents,
                    $this->transformContents($body['Contents'])
                );
            }

            if (isset($body['CommonPrefixes'])) {
                $contents = array_merge(
                    $contents,
                    $this->transformDirectories($body['CommonPrefixes'])
                );
            }
        } while ($nextMarker = $body['NextMarker'] ?? null);

        return $contents;
    }

    /**
     * @param string $directory
     * @param bool $recursive
     * @return array
     */
    protected function getListContentsQuery($directory, $recursive)
    {
        $query = [
            'max-keys' => 1000,
        ];

        if (! empty($directory)) {
            $directory = rtrim($directory, '/') . '/';
        }

        if ($prefix = $this->applyPathPrefix($directory)) {
            $query['prefix'] = $prefix;
        }

        if (! $recursive) {
            $query['delimiter'] = '/';
        }

        return $query;
    }

    /**
     * Transform contents.
     *
     * @param array $rawContents
     * @return array
     */
    protected function transformContents(array $rawContents)
    {
        if (! is_array(reset($rawContents))) {
            $rawContents = [$rawContents];
        }

        $contents = [];

        foreach ($rawContents as $rawContent) {
            if (substr($rawContent['Key'], -strlen('/')) === '/') {
                $type = 'dir';
            } else {
                $type = 'file';
            }

            $contents[] = [
                'type' => $type,
                'path' => $this->removePathPrefix($rawContent['Key']),
                'timestamp' => strtotime($rawContent['LastModified']),
            ];
        }

        return $contents;
    }

    /**
     * Transform directories.
     *
     * @param array $rawDirectories
     * @return array
     */
    protected function transformDirectories(array $rawDirectories)
    {
        if (! is_array(reset($rawDirectories))) {
            $rawDirectories = [$rawDirectories];
        }

        $directories = [];

        foreach ($rawDirectories as $rawDirectory) {
            $directories[] = [
                'type' => 'dir',
                'path' => $this->removePathPrefix($rawDirectory['Prefix']),
            ];
        }

        return $directories;
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string $path
     *
     * @return array|false
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function getMetadata($path)
    {
        $path = $this->applyPathPrefix($path);

        return $this->handleRequest(function () use ($path) {
            $response = $this->client->object->getObjectMeta($path, $this->options);

            return $this->transformFile($response, $path);
        });
    }

    /**
     * Get the size of a file.
     *
     * @param string $path
     *
     * @return array|false
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
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
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
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
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * @param \HuangYi\AliyunOss\Contracts\ResponseContract $response
     * @param string $path
     * @return array
     */
    protected function transformFile(ResponseContract $response, $path)
    {
        $file = [
            'type' => 'file',
            'path' => $path,
            'contents' => $response->getBody(),
        ];

        if ($response->hasHeader('Last-Modified')) {
            $file['timestamp'] = strtotime($response->getHeaderLine('Last-Modified'));
        }

        if ($response->hasHeader('Content-Length')) {
            $file['size'] = (int) $response->getHeaderLine('Content-Length');
        }

        if ($response->hasHeader('Content-Type')) {
            $file['mimetype'] = $response->getHeaderLine('Content-Type');
        }

        return $file;
    }

    /**
     * Write a new file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function write($path, $contents, Config $config)
    {
        $pathWithPrefix = rtrim($this->applyPathPrefix($path), '/');

        if ($config->has('ContentType')) {
            if (! isset($this->options['headers'])) {
                $this->options['headers'] = [];
            }

            $this->options['headers']['Content-Type'] = $config->get('ContentType');
        }

        $response = $this->client->object->putObject($pathWithPrefix, $contents, $this->options);

        return $this->transformFile($response, $path);
    }

    /**
     * Update a file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
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
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
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
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function copy($path, $newpath)
    {
        $path = $this->applyPathPrefix($path);
        $newpath = $this->applyPathPrefix($newpath);

        $this->client->object->copyObject($path, $newpath, null, $this->options);

        return true;
    }

    /**
     * Delete a file.
     *
     * @param string $path
     *
     * @return bool
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function delete($path)
    {
        $path = $this->applyPathPrefix($path);

        $this->client->object->deleteObject($path, $this->options);

        return true;
    }

    /**
     * Delete a directory.
     *
     * @param string $dirname
     *
     * @return bool
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function deleteDir($dirname)
    {
        $dirname = rtrim($dirname, '/') . '/';

        $contents = $this->listContents($dirname, true);

        $paths = [];

        foreach ($contents as $content) {
            $paths[] = $content['path'];
        }

        try {
            $this->client->object->deleteMultipleObjects($paths);
        } catch (RequestException $exception) {
            return false;
        }

        return true;
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
        $dirname = rtrim($this->applyPathPrefix($dirname), '/') . '/';

        try {
            $this->client->object->putObject($dirname, '', $this->options);
        } catch (RequestException $exception) {
            return false;
        }

        return ['type' => 'dir', 'path' => $dirname];
    }

    /**
     * Set the visibility for a file.
     *
     * @param string $path
     * @param string $visibility
     *
     * @return array|false
     */
    public function setVisibility($path, $visibility)
    {
        $pathWithPrefix = $this->applyPathPrefix($path);

        if ($visibility === AdapterInterface::VISIBILITY_PUBLIC) {
            $visibility = 'public-read';
        } elseif ($visibility === AdapterInterface::VISIBILITY_PRIVATE) {
            $visibility = 'private';
        }

        try {
            $this->client->object->putObjectAcl($pathWithPrefix, $visibility);
        } catch (RequestException $exception) {
            return false;
        }

        return ['visibility' => $visibility];
    }

    /**
     * Get the visibility of a file.
     *
     * @param string $path
     *
     * @return array|false
     *
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    public function getVisibility($path)
    {
        $path = $this->applyPathPrefix($path);

        return $this->handleRequest(function () use ($path) {
            $response = $this->client->object->getObjectAcl($path);

            $visibility = $response['AccessControlList']['Grant'];

            if ($visibility === 'private') {
                $visibility = AdapterInterface::VISIBILITY_PRIVATE;
            } elseif (in_array($visibility, ['public-read', 'public-read-write'], true)) {
                $visibility = AdapterInterface::VISIBILITY_PUBLIC;
            }

            return ['visibility' => $visibility];
        });
    }

    /**
     * Handle request.
     *
     * @param \Closure $callback
     * @return mixed
     * @throws \HuangYi\AliyunOss\Exceptions\RequestException
     */
    protected function handleRequest(Closure $callback)
    {
        try {
            return $callback();
        } catch (RequestException $exception) {
            if ($this->isFileNotFound($exception)) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * @param \Exception $exception
     * @return bool
     */
    protected function isFileNotFound($exception)
    {
        if (! $exception instanceof RequestException) {
            return false;
        }

        if (! $exception->hasResponse()) {
            return false;
        }
        if ($exception->getResponse()->getStatusCode() != 404) {
            return false;
        }

        return true;
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
     * @return \HuangYi\AliyunOss\OssClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Set OssClient.
     *
     * @param \HuangYi\AliyunOss\OssClient $client
     * @return $this
     */
    public function setClient(OssClient $client)
    {
        $this->client = $client;

        return $this;
    }
}
