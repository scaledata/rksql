package sqlapp

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/codahale/hdrhistogram"
	"github.com/jinzhu/gorm"
	"github.com/lib/pq"

	"rubrik/util/log"
)

type gormTx struct {
	gormDB *gorm.DB
}

func (db *gormTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	// NOTE: context is ignored.
	return nil, db.gormDB.Exec(query, args).Error
}

func (db *gormTx) Commit() error {
	return db.gormDB.Commit().Error
}

func (db *gormTx) Rollback() error {
	return db.gormDB.Rollback().Error
}

// ExecuteGORMTx executes fn inside a GORM transaction and retries it as needed
// using crdb utilities.
func ExecuteGORMTx(db *gorm.DB, fn func(tx *gorm.DB) error) error {
	tx := db.Begin()
	if err := tx.Error; err != nil {
		return err
	}
	return crdb.ExecuteInTx(context.Background(), &gormTx{tx}, func() error {
		return fn(tx)
	})
}

// RobustDB keeps retrying transactions and queries on errors
// Retryable function is used to decide which error to
// retry on
type RobustDB struct {
	gormDBs        []*gorm.DB
	sqlDBs         []*sql.DB
	retryable      func(error) bool
	retryDuration  time.Duration
	minWaitTime    time.Duration
	failed         int32
	success        int32
	retriesSuccess int32
	retriesTotal   int32
	latencyHisto   *hdrhistogram.Histogram
}

const maxWait = time.Second * 10

func newRobustDB(fn func(error) bool, args ...interface{}) *RobustDB {
	// Previously, RetryDurationDefault was one minute. Transactions would needlessly
	// fail because they would be trying to access a node that is injected with a
	// failure. One minute was not enough time to allow it to recover and restart the
	// transaction. It reduces the probability of the transaction choosing bad nodes
	// each time.
	const RetryDurationDefault = time.Minute * 5
	const MinWaitDefault = time.Nanosecond * 5
	rd := new(RobustDB)
	rd.retryable = fn
	rd.success = 0
	rd.failed = 0
	if len(args) > 0 {
		rd.retryDuration = args[0].(time.Duration)
	} else {
		rd.retryDuration = RetryDurationDefault
	}
	if len(args) > 1 {
		rd.minWaitTime = args[1].(time.Duration)
		if rd.minWaitTime > maxWait {
			log.Fatalf(
				context.TODO(),
				"minWaitTime %v cannot be greater than maxWaitTime %v",
				rd.minWaitTime,
				maxWait)
		}
	} else {
		rd.minWaitTime = MinWaitDefault
	}
	rd.latencyHisto = hdrhistogram.New(0, int64(rd.retryDuration), 4)
	return rd
}

// NewRobustDB returns a pointer to RobustDB struct which has dbs as slice of
// *gorm.DB and fn as retryable function, optionally third argument Max duration for
// retries and fourth argument minimum sleep time between retries can be specified.
// Sleep time will increase exponentially between retries.
func NewRobustDB(
	dbs []*gorm.DB,
	fn func(error) bool,
	args ...interface{},
) *RobustDB {
	rd := newRobustDB(fn, args...)
	rd.gormDBs = dbs
	log.Infof(context.TODO(), "NewRobustDB with pool of size %d", len(dbs))
	for i, db := range dbs {
		// source field is not exported
		log.Infof(context.TODO(), "pool[%d] -> %v", i, db)
	}
	return rd
}

// NewRobustSQLDB is similar to NewRobustDB except its dbs are *sql.DB instead
// of *gorm.DB.
func NewRobustSQLDB(
	dbs []*sql.DB,
	fn func(error) bool,
	args ...interface{},
) *RobustDB {
	rd := newRobustDB(fn, args...)
	rd.sqlDBs = dbs
	log.Infof(context.TODO(), "NewRobustSQLDB with pool of size %d", len(dbs))
	for i, db := range dbs {
		// source field is not exported
		log.Infof(context.TODO(), "pool[%d] -> %v", i, db)
	}
	return rd
}

// NumRetries returns the number of txn retries
func (rd *RobustDB) NumRetries() int32 {
	return rd.retriesTotal
}

// NumFailedTransaction returns number of failed transactions
func (rd *RobustDB) NumFailedTransaction() int32 {
	return rd.failed
}

// AvgRetries returns average number of tries per success transactions
func (rd *RobustDB) AvgRetries() float64 {
	return float64(rd.retriesSuccess) / float64(rd.success)
}

// NumSuccessfulTransaction returns number of successful transactions
func (rd *RobustDB) NumSuccessfulTransaction() int32 {
	return rd.success
}

// PrintStats prints statistics.
func (rd *RobustDB) PrintStats() {
	var buffer bytes.Buffer
	buffer.WriteString("RobustDB Statistics:\n")
	buffer.WriteString(fmt.Sprintf("  Total retries: %d\n", rd.NumRetries()))
	buffer.WriteString(
		fmt.Sprintf("  Successful txns: %d\n", rd.NumSuccessfulTransaction()))
	buffer.WriteString(
		fmt.Sprintf("  Failed txns: %d\n", rd.NumFailedTransaction()))
	buffer.WriteString(
		fmt.Sprintf("  Average Retries per succesful txn: %v\n", rd.AvgRetries()))
	h := rd.latencyHisto
	buffer.WriteString("  Txn Latency Stats (ns):\n")
	buffer.WriteString(fmt.Sprintf("    Total: %d\n", h.TotalCount()))
	buffer.WriteString(fmt.Sprintf("    Min: %v\n", h.Min()))
	buffer.WriteString(fmt.Sprintf("    Max: %v\n", h.Max()))
	buffer.WriteString(fmt.Sprintf("    Mean: %v\n", h.Mean()))
	buffer.WriteString(fmt.Sprintf("    P75: %v\n", h.ValueAtQuantile(75)))
	buffer.WriteString(fmt.Sprintf("    P99: %v\n", h.ValueAtQuantile(99)))
	log.Info(context.TODO(), buffer.String())
}

// RandomDB returns a randomly picked DB from DB list
func (rd *RobustDB) RandomDB() *gorm.DB {
	i := rand.Intn(len(rd.gormDBs))
	log.Infof(context.TODO(), "RobustDB.RandomDB chose DB at index %d", i)
	return rd.gormDBs[i]
}

// RandomSQLDB returns a randomly picked DB from DB list
func (rd *RobustDB) RandomSQLDB(ctx context.Context) *sql.DB {
	if len(rd.sqlDBs) == 0 {
		log.Fatal(ctx, "No SQL DBs")
	}
	i := rand.Intn(len(rd.sqlDBs))
	log.Infof(context.TODO(), "RobustDB.RandomDB chose DB at index %d", i)
	return rd.sqlDBs[i]
}

func (rd *RobustDB) nextWait(wait time.Duration) time.Duration {
	if wait < maxWait {
		wait *= 2
	} else {
		wait = maxWait
	}
	return wait
}

func (rd *RobustDB) retry(f func() error) error {
	var err error
	var retryCount int32

	defer func() {
		rd.retriesTotal += retryCount

	}()
	txStart := time.Now()
	deadline := txStart.Add(rd.retryDuration)
	wait := rd.minWaitTime
	for time.Now().Before(deadline) {
		retryStart := time.Now()
		retryCount++
		err = f()
		if err == nil {
			atomic.AddInt32(&rd.success, 1)
			rd.retriesSuccess += retryCount
			duration := time.Now().Sub(txStart)
			rd.latencyHisto.RecordValue(int64(duration))
			return nil
		}
		end := time.Now()
		log.Infof(
			context.TODO(),
			"ExecuteTx retry attempt %d failed, started at %v, now = %v, took %v",
			retryCount,
			retryStart,
			end,
			end.Sub(retryStart),
		)
		if !rd.retryable(err) {
			break
		}
		log.Infof(
			context.TODO(),
			"Attempt failed with error %s: ... Retrying after sleeping %v",
			err,
			wait,
		)
		time.Sleep(wait)
		wait = rd.nextWait(wait)
	}
	if !rd.retryable(err) {
		log.Infof(
			context.TODO(),
			"Aborting Retries because this error of type %T is not retryable : %s",
			err,
			err,
		)
		if e, ok := err.(*pq.Error); ok {
			log.Infof(
				context.TODO(),
				"postgres error code is %s and class is %s\n",
				e.Code,
				e.Code.Class(),
			)
		}
	} else {
		log.Infof(
			context.TODO(),
			"Aborting Retries because retry duration of %.0f seconds expired : %T : %s",
			rd.retryDuration.Seconds(),
			err,
			err,
		)
	}
	atomic.AddInt32(&rd.failed, 1)
	return err
}

// ExecuteTx takes a function which takes a gorm transaction and produces error,
// and on error keeps retrying to different DBs based on retryableError
// method of rd object
// crdb.ExecuteInTx is called internally by ExecuteGORMTx. It creates a SAVEPOINT,
// then it executes the user function, then calls RELEASE (equivalent of COMMIT in
// cockroach). If there is an error during RELEASE and it is not retryable,
// We get an AmbiguousCommitError. Here, we don't know weather transaction was commited or not.
// So, we cannot blindly retry on this error message.
func (rd *RobustDB) ExecuteTx(fn func(*gorm.DB) error) error {
	return rd.retry(
		func() error {
			db := rd.RandomDB()
			return ExecuteGORMTx(db, fn)
		},
	)
}

// ExecuteSQLTx is similar to ExecuteTx but takes a sql transaction instead of
// a GORM transaction.
func (rd *RobustDB) ExecuteSQLTx(
	ctx context.Context,
	fn func(*sql.Tx) error,
) error {
	return rd.retry(
		func() error {
			db := rd.RandomSQLDB(ctx)
			return crdb.ExecuteTx(ctx, db, nil, fn)
		},
	)
}

// ExecuteRawQuery executes raw single query functions with retry policies without transaction
func (rd *RobustDB) ExecuteRawQuery(fn func(*gorm.DB) error) error {
	return rd.retry(
		func() error {
			db := rd.RandomDB()
			return fn(db)
		},
	)
}

// ExecuteTraceOfQuery runs `SHOW TRACE FOR` on the parameter q and prints output in CSV format
func ExecuteTraceOfQuery(ctx context.Context, db *gorm.DB, q string) error {
	query := fmt.Sprintf("SHOW TRACE FOR %s", q)
	rows, err := db.DB().Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	log.Infof(ctx, "timestamp, age, message, context, operation, span")
	for rows.Next() {
		var timestamp, age, message, context, operation, span string
		if err := rows.Scan(&timestamp, &age, &message, &context, &operation, &span); err != nil {
			return err
		}
		log.Infof(ctx, "%s,%s,%s,%s,%s,%s", timestamp, age, message, context, operation, span)
	}
	return rows.Err()
}

// RetryAlways always retries on any error
func RetryAlways(_ error) bool {
	return true
}

// RetryNever never retries on any error
func RetryNever(_ error) bool {
	return false
}

// RetryOnConnectionError returns a function that retries only on connection
// error
func RetryOnConnectionError(err error) bool {
	ctx := context.TODO()
	switch e := err.(type) {
	case *net.OpError:
		return true
	case *pq.Error:
		return pqConnectionError(ctx, *e)
	case pq.Error:
		return pqConnectionError(ctx, e)
	case *crdb.TxnRestartError:
		return RetryOnConnectionError(e.Cause())
	default:
		if e == driver.ErrBadConn ||
			e == io.EOF ||
			e == pq.ErrSSLNotSupported {
			return true
		}
	}
	return false
}

func pqConnectionError(ctx context.Context, e pq.Error) bool {
	// See https://www.postgresql.org/docs/9.3/static/errcodes-appendix.html for
	// more detail
	log.Infof(
		ctx,
		"pq error - Error code : %s, Error class : %s\n",
		e.Code,
		e.Code.Class(),
	)
	switch string(e.Code.Class()) {
	case "08":
		//Connection Error
		return true
	case "57":
		//Operator intervention
		if e.Code == "57P01" {
			// This error is returned when cockroach is trying to shutdown
			// cleanly which is possible during a service restart. Consider
			// this as a connection error for purposes of retry.
			return true
		}
		return false
	case "58":
		//System Error (external or internal to cockroach)
		return true
	case "XX":
		//Internal Error
		return true
	// Currently no retries on these, raise an issue if you see any of these.
	case "01":
		//Warning
		return false
	case "02":
		//No Data
		return false
	case "03":
		//SQL statement not yet complete
		return false
	case "0A":
		//Feature Not Supported
		return false
	case "0B":
		//Invalid Transaction Initiation
		return false
	case "0F":
		//Locator Exception, What is this? Couldn't find on internet
		return false
	case "0L":
		//Invalid Grantor, What is this? Couldn't find on internet
		return false
	case "0P":
		//Invalid Role Specification, What is this? Couldn't find on internet
		return false
	case "0Z":
		//Diagnostice Exception
		return false
	case "20":
		//Case Not found
		return false
	case "21":
		//Cardinality Violation
		return false
	case "22":
		//Data Exception
		return false
	case "23":
		//Integrity Constraint Voliation
		return false
	case "24":
		//Invalid Cursor State
		return false
	case "25":
		//Invalid Transaction State
		return false
	case "26":
		//Invalid SQL Statement name
		return false
	case "27":
		//Triggered Data Change Violation
		return false
	case "28":
		//Invalid Authorization Specification
		return false
	case "2B":
		//Dependent Privilege Descriptors still exist. What is this??
		return false
	case "2D":
		//Invalid Transaction termination
		return false
	case "2F":
		//SQL routine exception
		return false
	case "34":
		//Invalid Cursor Name
		return false
	case "38":
		//External Routine Exception
		return false
	case "39":
		//External Routine Invocation Exception
		return false
	case "3B":
		//Savepoint Exception
		return false
	case "40":
		//Transaction rollback.
		return false
	case "42":
		//Syntax Error or Access rule violation
		return false
	case "53":
		//Insufficient resources(low_mem, disk_full, too_many_connections)
		return false
	case "55":
		//Object not in prerequisite state
		return false
	default:
		return false
	}
	return false
}
