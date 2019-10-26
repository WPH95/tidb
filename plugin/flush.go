package plugin

import (
	"github.com/pingcap/errors"
	//"github.com/pingcap/tidb/domain"
	"sync/atomic"
)

// NotifyFlush notify plugins to do flush logic.

// NotifyFlush notify plugins to do flush logic.
func NotifyFlush(dom interface{}, pluginName string) error {
	p := getByName(pluginName)
	if p == nil || p.Manifest.flushWatcher == nil || p.State != Ready {
		return errors.Errorf("plugin %s doesn't exists or unsupported flush or doesn't start with PD", pluginName)
	}
	//_, err := dom.GetEtcdClient().KV.Put(context.Background(), p.Manifest.flushWatcher.path, strconv.Itoa(int(p.Disabled)))
	//if err != nil {
	//	return err
	//}
	return nil
}

// ChangeDisableFlagAndFlush changes plugin disable flag and notify other nodes to do same change.
func ChangeDisableFlagAndFlush(dom interface{}, pluginName string, disable bool) error {
	p := getByName(pluginName)
	if p == nil || p.Manifest.flushWatcher == nil || p.State != Ready {
		return errors.Errorf("plugin %s doesn't exists or unsupported flush or doesn't start with PD", pluginName)
	}
	disableInt := uint32(0)
	if disable {
		disableInt = 1
	}
	atomic.StoreUint32(&p.Disabled, disableInt)
	//_, err := dom.GetEtcdClient().KV.Put(context.Background(), p.Manifest.flushWatcher.path, strconv.Itoa(int(disableInt)))
	//if err != nil {
	//	return err
	//}
	return nil
}


