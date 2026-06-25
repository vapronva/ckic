package caddy

import "fmt"

type LoadBalancerMode int

const (
	LoadBalancerModeNone LoadBalancerMode = iota
	LoadBalancerModeCilium
)

const (
	loadBalancerModeNoneName   = "none"
	loadBalancerModeCiliumName = "cilium"
)

func (m LoadBalancerMode) String() string {
	switch m {
	case LoadBalancerModeNone:
		return loadBalancerModeNoneName
	case LoadBalancerModeCilium:
		return loadBalancerModeCiliumName
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
	default:
		return LoadBalancerModeNone, fmt.Errorf(
			"invalid loadbalancer mode %q (want none or cilium)", s,
		)
	}
}
