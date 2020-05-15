package journal

import (
	"context"

	"github.com/ceph/ceph-csi/internal/util"

	"github.com/ceph/go-ceph/rados"
	"k8s.io/klog"
)

// listExcess is the number of false-positve key-value pairs we will
// accept from ceph when getting omap values.
var listExcess = 32

func getOMapValues(
	ctx context.Context,
	conn *Connection,
	poolName, namespace, oid, prefix string, keys []string) (map[string]string, error) {
	// fetch and configure the rados ioctx
	ioctx, err := conn.conn.GetIoctx(poolName)
	if err != nil {
		return nil, omapPoolError(poolName, err)
	}
	defer ioctx.Destroy()

	if namespace != "" {
		ioctx.SetNamespace(namespace)
	}

	results := map[string]string{}
	// want is our "lookup map" that ensures O(1) checks for keys
	// while iterating, without needing to complicate the caller.
	want := make(map[string]bool, len(keys))
	for i := range keys {
		want[keys[i]] = true
	}

	err = ioctx.ListOmapValues(
		oid, "", prefix, int64(len(want)+listExcess),
		func(key string, value []byte) {
			if want[key] {
				results[key] = string(value)
			}
		},
	)
	switch err {
	case nil:
	case rados.ErrNotFound:
		klog.Errorf(
			util.Log(ctx, "omap not found (pool=%q, namespace=%q, name=%q): %v"),
			poolName, namespace, oid, err)
		return nil, util.NewErrKeyNotFound(oid, err)
	default:
		return nil, err
	}

	klog.Infof(
		util.Log(ctx, "XXX found in omap: (pool=%q, namespace=%q, name=%q): %v"),
		poolName, namespace, oid, results)
	return results, nil
}


func removeOneOMapKey(
	ctx context.Context,
	conn *Connection,
	poolName, namespace, oMapName, oMapKey string) error {
	// fetch and configure the rados ioctx
	ioctx, err := conn.conn.GetIoctx(poolName)
	if err != nil {
		return omapPoolError(poolName, err)
	}
	defer ioctx.Destroy()

	if namespace != "" {
		ioctx.SetNamespace(namespace)
	}

	err = ioctx.RmOmapKeys(oMapName, []string{oMapKey})
	if err != nil {
		klog.Errorf(
			util.Log(ctx, "failed removing omap key (pool=%q, namespace=%q, name=%q, key=%q): %v"),
			poolName, namespace, oMapName, oMapKey, err)
	} else {
		klog.Infof(
			util.Log(ctx, "XXX removed omap key (pool=%q, namespace=%q, name=%q, key=%q, ): %v"),
			poolName, namespace, oMapName, oMapKey, err)
	}
	return err
}

func setOneOMapKey(
	ctx context.Context,
	conn *Connection,
	poolName, namespace, oMapName, oMapKey, keyValue string) error {
	// fetch and configure the rados ioctx
	ioctx, err := conn.conn.GetIoctx(poolName)
	if err != nil {
		return omapPoolError(poolName, err)
	}
	defer ioctx.Destroy()

	if namespace != "" {
		ioctx.SetNamespace(namespace)
	}

	pairs := map[string][]byte{
		oMapKey: []byte(keyValue),
	}
	err = ioctx.SetOmap(oMapName, pairs)
	if err != nil {
		klog.Errorf(
			util.Log(ctx, "failed setting omap key (pool=%q, namespace=%q, name=%q, key=%q, value=%q): %v"),
			poolName, namespace, oMapName, oMapKey, keyValue, err)
	} else {
		klog.Infof(
			util.Log(ctx, "XXX set omap key (pool=%q, namespace=%q, name=%q, key=%q, value=%q): %v"),
			poolName, namespace, oMapName, oMapKey, keyValue, err)
	}
	return err
}

func omapPoolError(poolName string, err error) error {
	if err == rados.ErrNotFound {
		return util.NewErrPoolNotFound(poolName, err)
	}
	return err
}
