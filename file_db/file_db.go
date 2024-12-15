package filedb

import (
	"fmt"
	"math/rand"
	"os"
)

// SaveDataV1 saves the data to the file.
// Drawbacks:
// - It truncates the file if it exists befure updating it.
// - Writing data to be file may not be atomic
// - When is the data actually persisted to the disk
func SaveDataV1(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return err
	}
	return nil

}

// SaveDataV1 saves the data to a temporary file and then renames it to the original file.
//
// Improvements:
// - rename operation is atomic - if the system crashes before renaming the original file,
// the original file renames intact.
//
// Drawbacks:
//   - The whole operation is not atomic. If the system crashes after renaming the file,
//   - When does the data is actually persisted to the disk
//     -The file metadata not be persisted to the disk with the data
func SaveData2(path string, data []byte) (er error) {
	temp := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	file, err := os.OpenFile(temp, os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		return err
	}
	defer func() {
		file.Close()
		if err != nil {

			os.Remove(temp)
		}
	}()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return os.Rename(temp, path)
}

// SaveDataV3 saves the data to a temporary file, flush the data to disk and
// then renames it to the original file.
//
// Improvements:
// - The data is flushed to the disk before renaming the file.
//
// Drawbacks:
// - We have flush the data to disk but what about metadata
func SaveDataV3(path string, data []byte) (er error) {

	temp := fmt.Sprintf("%s.tmp.%d", path, rand.Int())
	file, err := os.OpenFile(temp, os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		return err
	}
	defer func() {
		file.Close()
		// If there is an error, remove the temporary file
		if err != nil {
			os.Remove(temp)
		}
	}()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}

	return os.Rename(temp, path)
}
