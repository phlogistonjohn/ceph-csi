package journal

import (
	"context"

	"github.com/ceph/ceph-csi/internal/util"

	"github.com/ceph/go-ceph/rados"
	"k8s.io/klog"
)

func getOneOMapValue(
	ctx context.Context,
	conn *Connection,
	poolName, namespace, oMapName, oMapKey string) (string, error) {

	ioctx, err := conn.conn.GetIoctx(poolName)
	if err != nil {
		return "", err
	}
	defer ioctx.Destroy()
	if namespace != "" {
		ioctx.SetNamespace(namespace)
	}

	pairs, err := ioctx.GetOmapValues(
		oMapName, // oid (name of object)
		"",       // startAfter (ignored)
		oMapKey,  // filterPrefix - match only keys with this prefix
		1,        // maxReturn - fetch no more than N values
	)
	switch err {
	case nil:
	case rados.RadosErrorNotFound:
		klog.Errorf(
			util.Log(ctx, "omap not found (pool=%q, namespace=%q, name=%q, key=%q): %v"),
			poolName, namespace, oMapName, oMapKey, err)
		return "", util.NewErrKeyNotFound(oMapKey, err)
	default:
		return "", err
	}

	result, found := pairs[oMapKey]
	if !found {
		klog.Errorf(
			util.Log(ctx, "key not found in omap (pool=%q, namespace=%q, name=%q, key=%q): %v"),
			poolName, namespace, oMapName, oMapKey, err)
		return "", util.NewErrKeyNotFound(oMapKey, nil)
	}
	klog.Infof(
		util.Log(ctx, "key found in omap! (pool=%q, namespace=%q, name=%q, key=%q): %v"),
		poolName, namespace, oMapName, oMapKey, result)
	return string(result), nil
}
