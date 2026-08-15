package saga

type PortOut[ID, T, CT any] interface {
	CommandAlreadyHandled(id ID) (bool, error)
	MarkCommandAsHandled(id ID) error
	TransactionCompensated(tx T) (bool, error)
}
