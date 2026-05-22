package caddy

import "fmt"

type LoadBalancerMode int

const (
	LoadBalancerModeNone LoadBalancerMode = iota
	LoadBalancerModeCilium
	LoadBalancerModeShared
)

const (
	loadBalancerModeNoneName   = "none"
	loadBalancerModeCiliumName = "cilium"
	loadBalancerModeSharedName = "shared"
)

func (m LoadBalancerMode) String() string {
	switch m {
	case LoadBalancerModeNone:
		return loadBalancerModeNoneName
	case LoadBalancerModeCilium:
		return loadBalancerModeCiliumName
	case LoadBalancerModeShared:
		return loadBalancerModeSharedName
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

func ParseLoadBalancerMode(s string) (LoadBalancerMode, error) {
	switch s {
	case loadBalancerModeNoneName:
		return LoadBalancerModeNone, nil
	case loadBalancerModeCiliumName:
		return LoadBalancerModeCilium, nil
	case loadBalancerModeSharedName:
		return LoadBalancerModeShared, nil
	default:
		return LoadBalancerModeNone, fmt.Errorf(
			"invalid loadbalancer mode %q (want none, cilium, or shared)", s,
		)
	}
}
