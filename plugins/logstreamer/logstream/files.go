package logstream

import (
	"fmt"
	"os"
	"path/filepath"

	log "github.com/Sirupsen/logrus"

	"github.com/ekini/tail"
	"github.com/howeyc/fsnotify"
)

func walkLogDir(dir string) (files []string, err error) {
	log.Debugf("Walking %s", dir)
	if string(dir[len(dir)-1]) != "/" {
		dir = dir + "/"
	}
	visit := func(path string, f os.FileInfo, err error) error {
		if f.IsDir() {
			return nil
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			log.Fatalf("Can't get abs filepath: %s", err)
		}
		files = append(files, abs)
		return nil
	}
	err = filepath.Walk(dir, visit)
	return
}

func WatchFiles(dirs []string, groups []Group, continuous, readOnce bool) chan Metric {

	// get list of all files in watch dir
	files := make([]string, 0)
	for _, dir := range dirs {
		fs, err := walkLogDir(dir)
		if err != nil {
			panic(err)
		}
		for _, f := range fs {
			files = append(files, f)
		}
	}

	// assign file per group
	assignedFiles, err := assignFiles(files, groups)
	if err != nil {
		log.Fatalln("can't assign file per group", err)
	}

	doneCh := make(chan string)
	assignedFilesCount := len(assignedFiles)
	log.Debugf("Assigned files count: %d", assignedFilesCount)

	for _, file := range assignedFiles {
		file.doneCh = doneCh
		go file.tail()
	}

	if continuous {
		for _, dir := range dirs {
			go continueWatch(&dir, groups)
		}
	}

	go func() {
		for name := range doneCh {
			log.Debugf("Finished reading %s", name)
		}
	}()

	return metricsChan

}

func assignFiles(files []string, groups []Group) (outFiles []*File, err error) {
	for n, group := range groups {
		log.Debugf("Assigning files for group: %s", group.Mask)
		var assignedFiles []*File
		if group.Name == "" {
			group.Name = fmt.Sprintf("group%d", n)

		}
		if assignedFiles, err = getFilesByGroup(files, &group); err == nil {
			for _, assignedFile := range assignedFiles {
				outFiles = append(outFiles, assignedFile)
			}
		} else {
			return
		}
	}
	return
}

func getFilesByGroup(allFiles []string, group *Group) ([]*File, error) {
	files := make([]*File, 0)
	regex := *group.Mask
	for _, f := range allFiles {
		if !regex.MatchString(filepath.Base(f)) {
			continue
		}
		file, err := NewFile(f, group)
		if err != nil {
			return files, err
		}
		files = append(files, file)
	}
	return files, nil
}

func continueWatch(dir *string, groups []Group) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Can't create watcher: %s", err)
	}

	done := make(chan bool)

	// Process events
	go func() {
		for {
			select {
			case ev := <-watcher.Event:
				if ev.IsCreate() {
					files := make([]string, 0)
					file, err := filepath.Abs(ev.Name)
					if err != nil {
						log.Printf("can't get file %+v", err)
						continue
					}
					files = append(files, file)
					log.Debugf("Assigning %s", file)
					assignFiles(files, groups)
				}
			case err := <-watcher.Error:
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Watch(*dir)
	if err != nil {
		log.Fatalf("Can't watch directory: %s", err)
	}

	<-done

	/* ... do stuff ... */
	watcher.Close()
}

func NewFile(fpath string, group *Group) (*File, error) {
	file := &File{group: group}
	var (
		err    error
		whence int
	)
	logger := log.StandardLogger()

	// if we have DateFormat, then we can read file as many times as we want
	// without duplicating points
	if group.DateFormat != "" {
		log.Debugf("Read whole file from the beginning: %+v", fpath)
		whence = 0
	} else {
		log.Debugf("Read file from the end: %+v", fpath)
		whence = 2
	}
	seekInfo := &tail.SeekInfo{Offset: 0, Whence: whence}

	file.Tail, err = tail.TailFile(fpath, tail.Config{Follow: true, ReOpen: true, Location: seekInfo, Logger: logger})
	return file, err
}

type File struct {
	Tail   *tail.Tail
	group  *Group
	doneCh chan string
}

func (f *File) tail() {
	log.Printf("Start tailing %+v", f.Tail.Filename)
	defer func() { f.doneCh <- f.Tail.Filename }()
	for line := range f.Tail.Lines {
		f.checkLineRules(&line.Text)
	}
}

func (f *File) checkLineRule(line *string, rule *Rule) {
	match := rule.Match(line)
	if match != nil {
		rule.process(f.group, match)
	}
}

func (f *File) checkLineRules(line *string) {
	for _, rule := range f.group.Rules {
		f.checkLineRule(line, rule)
	}
}
