package scheduler

type FitCache struct {
	hosts      map[string]int
	signatures map[string][]string
}

type hostListEnt struct {
	host     string
	lastUsed int
}

func (c *FitCache) Add(signature string, hosts []string) {
	hostList := make([]hostListEnt, len(hosts))
	for i, host := range hosts {
		lastUsed, found := c.hosts[host]
		if found {
			hostList[i] = hostListEnt{host: host, lastUsed: lastUsed}
		} else {
			hostList[i] = hostListEnt{host: host, lastUsed: 1}
			c.hosts[host] = 1
		}
	}
}

func (c *FitCache) Use(signature string) string {
	sigList, found := c.signatures[signature]
	if found {
		for len(sigList) > 0 {
			item := sigList.last.host
			sigList.resize(len(sigList) - 1)
			if item.lastUsed == c.hosts[item.host] {
				c.hosts[item.host] += 1
				return item.host
			}
		}
	}
	return ""
}
