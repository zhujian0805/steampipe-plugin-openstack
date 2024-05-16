package openstack

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func Plugin(ctx context.Context) *plugin.Plugin {
	p := &plugin.Plugin{
		Name:             "steampipe-plugin-openstack_legacy",
		DefaultTransform: transform.FromGo().NullIfZero(),
		DefaultIgnoreConfig: &plugin.IgnoreConfig{
			ShouldIgnoreErrorFunc: shouldIgnoreErrors([]string{"404", "ErrDefault404", "itemNotFound"}),
		},
		TableMap: map[string]*plugin.Table{
			"openstack_instance":            tableOpenStackInstance(ctx),
			"openstack_project":             tableOpenStackProject(ctx),
			"openstack_user":                tableOpenStackUser(ctx),
			"openstack_port":                tableOpenStackPort(ctx),
			"openstack_volume":              tableOpenStackVolume(ctx),
			"openstack_attachment":          tableOpenStackAttachment(ctx),
			"openstack_image":               tableOpenStackImage(ctx),
			"openstack_security_group":      tableOpenStackSecurityGroup(ctx),
			"openstack_security_group_rule": tableOpenStackSecurityGroupRule(ctx),
			"security_group_rule":           tableSecurityGroupRule(ctx),
			"openstack_network":             tableOpenStackNetwork(ctx),
			"openstack_subnet":              tableOpenStackSubnet(ctx),
			"openstack_hypervisor":          tableOpenStackHypervisor(ctx),
			"openstack_aggregate":           tableOpenStackAggregate(ctx),
			"openstack_flavor":              tableOpenStackFlavor(ctx),
			"openstack_loadbalancer":        tableOpenStackLoadBalancer(ctx),
			"openstack_listener":            tableOpenStackListener(ctx),
			"openstack_pool":                tableOpenStackPool(ctx),
			"openstack_pool_member":         tableOpenStackPoolMember(ctx),
		},
		ConnectionConfigSchema: &plugin.ConnectionConfigSchema{
			NewInstance: ConfigInstance,
			Schema:      ConfigSchema,
		},
	}
	return p
}
