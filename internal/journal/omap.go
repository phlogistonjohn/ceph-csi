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
	// fetch and configure the rados ioctx
	ioctx, err := conn.conn.GetIoctx(poolName)
	if err != nil {
		return "", omapPoolError(poolName, err)
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
	case rados.ErrNotFound:
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
		util.Log(ctx, "XXX key found in omap! (pool=%q, namespace=%q, name=%q, key=%q): %v"),
		poolName, namespace, oMapName, oMapKey, result)
	return string(result), nil
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
