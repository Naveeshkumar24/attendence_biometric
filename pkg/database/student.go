package database

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/VsenseTechnologies/biometric_http_server/internals/models"
	"github.com/VsenseTechnologies/biometric_http_server/pkg/utils"
)

func isValidIdentifier(id string) bool {
	re := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	return re.MatchString(id)
}
func (q *Query) CheckStudentUnitIdExists(unitId string, studentUnitId string) (bool, error) {
	query := `SELECT EXISTS ( SELECT 1 FROM  ` + unitId + ` WHERE student_unit_id = $1)`

	var isStudentUnitIdExists bool

	if err := q.db.QueryRow(query, studentUnitId).Scan(&isStudentUnitIdExists); err != nil {
		return false, err
	}

	return isStudentUnitIdExists, nil
}

func (q *Query) CreateNewStudent(student *models.Student, unitId string, fingerPrintData string) error {
	query1 := `INSERT INTO fingerprintdata (student_id,student_unit_id,unit_id,fingerprint) VALUES ($1,$2,$3,$4)`
	query2 := fmt.Sprintf("INSERT INTO %s (student_id,student_unit_id,student_name,student_usn,department) VALUES ($1,$2,$3,$4,$5)", unitId)
	query3 := `INSERT INTO inserts (unit_id,student_unit_id,fingerprint_data) VALUES ($1,$2,$3)`

	tx, err := q.db.Begin()

	if err != nil {
		return err
	}

	if _, err := tx.Exec(query1, student.StudentId, student.StudentUnitId, unitId, fingerPrintData); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Exec(query2, student.StudentId, student.StudentUnitId, student.StudentName, student.StudentUsn, student.Department); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Exec(query3, unitId, student.StudentUnitId, fingerPrintData); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

func (q *Query) UpdateStudent(unitId string, studentId string, studentName string, studentUsn string, department string) error {
	query := fmt.Sprintf(`UPDATE %s SET student_name=$2,student_usn=$3,department=$4 WHERE student_id=$1`, unitId)
	if _, err := q.db.Exec(query, studentId, studentName, studentUsn, department); err != nil {
		return err
	}
	return nil
}

func (q *Query) DeleteStudent(unitId string, studentId string, studentUnitId string) error {
	query1 := `DELETE FROM fingerprintdata WHERE student_id=$1`
	query2 := `INSERT INTO deletes (unit_id,student_unit_id) VALUES ($1,$2)`
	query3 := `DELETE FROM inserts WHERE unit_id=$1 AND student_unit_id=$2`

	tx, err := q.db.Begin()

	if err != nil {
		return err
	}

	if _, err := tx.Exec(query1, studentId); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Exec(query2, unitId, studentUnitId); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.Exec(query3, unitId, studentUnitId); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (q *Query) GetStudentDetails(unitId string) ([]*models.Student, error) {
	query := fmt.Sprintf(`SELECT student_id,student_unit_id,student_name,student_usn,department FROM %s`, unitId)

	var students []*models.Student

	rows, err := q.db.Query(query)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var student models.Student

		if err := rows.Scan(&student.StudentId, &student.StudentUnitId, &student.StudentName, &student.StudentUsn, &student.Department); err != nil {
			return nil, err
		}

		students = append(students, &student)
	}

	return students, nil
}

func (q *Query) GetStudentLogs(studentId string) ([]*models.StudentAttendanceLog, error) {
	query := `
			SELECT date, login, logout 
				FROM attendance 
				WHERE student_id = $1 
			ORDER BY date DESC;
			`

	rows, err := q.db.Query(query, studentId)

	var studentLogs []*models.StudentAttendanceLog

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var attendanceLog models.StudentAttendanceLog

		if err := rows.Scan(&attendanceLog.Date, &attendanceLog.LoginTime, &attendanceLog.LogoutTime); err != nil {
			return nil, err
		}

		if attendanceLog.LoginTime != "25:00" {
			t1, err := utils.ConvertTo12HourFormat(attendanceLog.LoginTime)

			if err != nil {
				return nil, err
			}

			attendanceLog.LoginTime = t1

		}

		if attendanceLog.LogoutTime != "25:00" {
			t2, err := utils.ConvertTo12HourFormat(attendanceLog.LogoutTime)

			if err != nil {
				return nil, err
			}
			attendanceLog.LogoutTime = t2
		}
		studentLogs = append(studentLogs, &attendanceLog)
	}

	return studentLogs, nil
}

func (q *Query) GetStudentsCountFromUnit(unitId string) (int32, error) {
	query := `SELECT COUNT(*) FROM ` + unitId

	var studentCount int32

	if err := q.db.QueryRow(query).Scan(&studentCount); err != nil {
		return -1, err
	}

	return studentCount, nil

}

func (q *Query) GetUserStandardTime(userId string) (*models.UserTime, error) {
	query := `SELECT morning_start,morning_end,afternoon_start,afternoon_end,evening_start,evening_end FROM times where user_id=$1`
	var userTime models.UserTime
	if err := q.db.QueryRow(query, userId).Scan(
		&userTime.MorningStart,
		&userTime.MorningEnd,
		&userTime.AfterNoonStart,
		&userTime.AfterNoonEnd,
		&userTime.EveningStart,
		&userTime.EveningEnd,
	); err != nil {
		return nil, err
	}
	return &userTime, nil
}

func (q *Query) GetStudentsForPdf(unitId string, studentsCount int32) (map[string]*models.PdfFormat, error) {
	query := `SELECT student_id, student_name, student_usn FROM ` + unitId + ` ORDER BY student_name`

	rows, err := q.db.Query(query)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var pdfFormats = make(map[string]*models.PdfFormat, studentsCount)

	for rows.Next() {
		var pdfFormat models.PdfFormat
		if err := rows.Scan(&pdfFormat.StudentId, &pdfFormat.Name, &pdfFormat.Usn); err != nil {
			return nil, err
		}

		pdfFormats[pdfFormat.StudentId] = &pdfFormat
	}

	return pdfFormats, nil
}

func (q *Query) GetStudentsAttendanceLogForPdf(
	studentsCount int32,
	userTime *models.UserTime,
	pdfFormats map[string]*models.PdfFormat,
	date string,
	slot string,
) error {
	var wg sync.WaitGroup
	var mu sync.Mutex

	for studentId := range pdfFormats {
		wg.Add(1)
		id := studentId // capture loop variable

		go func(id string) {
			defer wg.Done()

			rows, err := q.db.Query(
				`SELECT login, logout FROM attendance WHERE date=$1 AND student_id=$2`,
				date, id,
			)
			if err != nil {
				// on error, mark as pending
				mu.Lock()
				pdfFormats[id].Login = "pending"
				pdfFormats[id].Logout = "pending"
				mu.Unlock()
				return
			}
			defer rows.Close()

			var (
				found         bool
				login, logout string
			)

			// scan through all entries for this student/date
			for rows.Next() {
				if err := rows.Scan(&login, &logout); err != nil {
					break
				}
				if logout == "25:00" {
					continue
				}

				// choose the correct time window
				var startBound, endBound string
				switch slot {
				case "morning":
					startBound, endBound = userTime.MorningStart, userTime.AfterNoonEnd
				case "evening":
					startBound, endBound = userTime.AfterNoonStart, userTime.EveningEnd
				default: // full day
					startBound, endBound = userTime.MorningStart, userTime.EveningEnd
				}

				valid, _ := utils.CompareWithStandardTime(
					startBound, endBound, login, logout,
				)
				if valid {
					found = true
					break
				}
			}

			// write back under lock
			mu.Lock()
			if found {
				pdfFormats[id].Login = login
				pdfFormats[id].Logout = logout
			} else {
				pdfFormats[id].Login = "pending"
				pdfFormats[id].Logout = "pending"
			}
			mu.Unlock()

		}(id)
	}

	wg.Wait()
	return nil
}
