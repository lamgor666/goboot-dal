package DB

type Exception struct {
	errorTips string
}

func NewException(errorTips string) Exception {
	return Exception{errorTips: errorTips}
}

func (ex Exception) Error() string {
	return ex.errorTips
}

func toDbException(err error) Exception {
	if ex, ok := err.(Exception); ok {
		return ex
	}

	return Exception{errorTips: err.Error()}
}
