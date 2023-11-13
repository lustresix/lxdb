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

func deleteData(data []string) error {
	for _, key := range data {
		err := db.Delete([]byte(key))
		if err != nil {
			return err
		}
	}
	return nil
}

func ListKey() []string {
	keys := db.ListKeys()
	var key []string
	for _, i := range keys {
		key = append(key, string(i))
	}
	return key
}
