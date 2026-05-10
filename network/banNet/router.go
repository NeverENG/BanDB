package banNet

import (
	"github.com/NeverENG/BanDB/network/banIface"
)

type BaseRouter struct{}

var _ banIface.IRouter = &BaseRouter{}

func (B *BaseRouter) PreHandle(req banIface.IRequest) {}

func (B *BaseRouter) Handle(req banIface.IRequest) {}

func (B *BaseRouter) PostHandle(req banIface.IRequest) {}
