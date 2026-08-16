package openaiapi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const redisProtocolPrefix = "weknora:openai-protocol:"
const redisResponsesSyncMaxOutputUnsupportedPrefix = "weknora:openai-capability:responses-sync-max-output-unsupported:"
const redisChatMaxCompletionNeutralPrefix = "weknora:openai-capability:chat-max-completion-neutral:"

type redisProtocolStore struct {
	client *redis.Client
}

func NewRedisProtocolStore(client *redis.Client) ProtocolStore {
	if client == nil {
		return nil
	}
	return &redisProtocolStore{client: client}
}

func redisProtocolKey(cacheKey string) string {
	return redisProtocolPrefix + normalizeProtocolCacheLookupKey(cacheKey)
}

func (s *redisProtocolStore) Load(ctx context.Context, cacheKey string) (Protocol, bool, error) {
	value, err := s.client.Get(ctx, redisProtocolKey(cacheKey)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	protocol := Protocol(value)
	if !validProtocol(protocol) {
		return "", false, nil
	}
	return protocol, true, nil
}

func (s *redisProtocolStore) Save(ctx context.Context, cacheKey string, protocol Protocol) error {
	if !validProtocol(protocol) {
		return nil
	}
	return s.client.Set(ctx, redisProtocolKey(cacheKey), string(protocol), 0).Err()
}

func (s *redisProtocolStore) LoadResponsesSyncMaxOutputUnsupported(
	ctx context.Context, cacheKey string,
) (bool, error) {
	value, err := s.client.Get(
		ctx,
		redisResponsesSyncMaxOutputUnsupportedPrefix+normalizeProtocolCacheLookupKey(cacheKey),
	).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "1", nil
}

func (s *redisProtocolStore) SaveResponsesSyncMaxOutputUnsupported(
	ctx context.Context, cacheKey string,
) error {
	return s.client.Set(
		ctx,
		redisResponsesSyncMaxOutputUnsupportedPrefix+normalizeProtocolCacheLookupKey(cacheKey),
		"1",
		0,
	).Err()
}

func (s *redisProtocolStore) LoadChatMaxCompletionNeutral(
	ctx context.Context, cacheKey string,
) (bool, error) {
	value, err := s.client.Get(
		ctx,
		redisChatMaxCompletionNeutralPrefix+normalizeProtocolCacheLookupKey(cacheKey),
	).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "1", nil
}

func (s *redisProtocolStore) SaveChatMaxCompletionNeutral(
	ctx context.Context, cacheKey string,
) error {
	return s.client.Set(
		ctx,
		redisChatMaxCompletionNeutralPrefix+normalizeProtocolCacheLookupKey(cacheKey),
		"1",
		0,
	).Err()
}
