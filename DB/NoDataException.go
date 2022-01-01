package DB

type NoDataException struct {
}

func NewNoDataException() NoDataException {
	return NoDataException{}
}

func (ex NoDataException) Error() string {
	return "no data"
}
