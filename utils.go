package sio

import "github.com/funcards/socket.io-parser/v5"

func CreateDataPacket(t siop.PacketType, event string, args ...any) siop.Packet {
	var data []any

	if len(event) > 0 {
		data = append(data, event)
	}

	if len(args) > 0 {
		data = append(data, args...)
	}

	return siop.Packet{
		Type: t,
		Data: data,
	}
}
