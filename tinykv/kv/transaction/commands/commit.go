package commands

import (
	"encoding/hex"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"reflect"
)

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion
	if commitTs <= c.request.StartVersion {
		return nil, fmt.Errorf("invalid transaction timestamp: %d (commit TS) <= %d (start TS)", commitTs, c.request.StartVersion)
	}

	response := new(kvrpcpb.CommitResponse)

	// Commit each key.
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	// If there is no correspond lock for this transaction.
	if lock == nil || lock.Ts != txn.StartTS {
		// Key is locked by a different transaction, or there is no lock on the key. It's needed to
		// check the commit/rollback record for this key, if nothing is found report lock not found
		// error.
		write, recordCommitTS, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				log.Warn("the transaction is already rolled back",
					zap.Uint64("startTS", txn.StartTS),
					zap.String("key", hex.EncodeToString(key)))
				if recordCommitTS != write.StartTS {
					log.Fatal("unexpected write record the start and commit ts for a rollback record"+
						"should be the same", zap.Uint64("startTS", txn.StartTS),
						zap.Uint64("recordCommitTS", recordCommitTS),
						zap.String("key", hex.EncodeToString(key)))
				}
				respValue := reflect.ValueOf(response)
				keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("the key %v is already rolled back", key)}
				reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
				return response, nil
			} else {
				log.Debug("the transaction is already committed",
					zap.Uint64("startTS", txn.StartTS),
					zap.String("key", hex.EncodeToString(key)),
					zap.Uint64("commitTS", recordCommitTS))
				return nil, nil
			}
		}
		respValue := reflect.ValueOf(response)
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
	}

	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
