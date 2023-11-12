package main

func putData(data map[string]string) error {
	for key, value := range data {
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			return err
		}
	}
	return nil
}

func getValue(data []string) ([]string, error) {
	var values []string
	for _, key := range data {
		value, err := db.Get([]byte(key))
		values = append(values, string(value))
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}
