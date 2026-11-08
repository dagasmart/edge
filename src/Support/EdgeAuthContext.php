<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Support;

use Swow\Coroutine;

final class EdgeAuthContext
{
    /** @var array<int, array<string, mixed>> */
    private static array $contexts = [];

    private const string KEY_USERNAME = 'username';
    private const string KEY_CLIENT_ID = 'client_id';
    private const string KEY_REASON = 'reason';

    private function __construct() {}

    public static function setAuth(string $username, string $clientId): void
    {
        $cid = Coroutine::getCurrent()->getId();
        self::$contexts[$cid] = [
            self::KEY_USERNAME => $username,
            self::KEY_CLIENT_ID => $clientId,
        ];
    }

    public static function getUsername(): ?string
    {
        $ctx = self::$contexts[Coroutine::getCurrent()->getId()] ?? null;

        return $ctx[self::KEY_USERNAME] ?? null;
    }

    public static function getClientId(): ?string
    {
        $ctx = self::$contexts[Coroutine::getCurrent()->getId()] ?? null;

        return $ctx[self::KEY_CLIENT_ID] ?? null;
    }

    public static function setAuthReason(string $reason): void
    {
        $cid = Coroutine::getCurrent()->getId();
        if (!isset(self::$contexts[$cid])) {
            self::$contexts[$cid] = [];
        }
        self::$contexts[$cid][self::KEY_REASON] = $reason;
    }

    public static function getAuthReason(): ?string
    {
        $ctx = self::$contexts[Coroutine::getCurrent()->getId()] ?? null;

        return $ctx[self::KEY_REASON] ?? null;
    }

    /**
     * 清理当前协程上下文
     * Swow 2.0 兼容：由 server.php finally 调用，不使用 Coroutine::defer
     */
    public static function clear(): void
    {
        $cid = Coroutine::getCurrent()->getId();
        unset(self::$contexts[$cid]);
    }
}
