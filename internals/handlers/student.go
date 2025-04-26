package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/VsenseTechnologies/biometric_http_server/internals/models"
)

type studentHandler struct {
	repo models.StudentInterface
}

func NewStudentHandler(repo models.StudentInterface) *studentHandler {
	return &studentHandler{
		repo,
	}
}

func (h *studentHandler) CreateNewStudentHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := h.repo.CreateNewStudent(r); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "student created successfully"})
}

func (h *studentHandler) UpdateStudentDetailsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := h.repo.UpdateStudentDetails(r); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "student details updated successfully"})
}

func (h *studentHandler) DeleteStudentHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := h.repo.DeleteStudent(r); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "student deleted successfully"})
}

func (h *studentHandler) GetStudentDetailsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	students, err := h.repo.GetStudentDetails(r)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	if students == nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"students": []interface{}{},
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string][]*models.Student{
		"students": students,
	})

}

func (h *studentHandler) GetStudentLogsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	logs, err := h.repo.GetStudentLogs(r)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	if logs == nil {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(
			map[string]interface{}{
				"logs": []interface{}{},
			},
		)

		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string][]*models.StudentAttendanceLog{
		"logs": logs,
	})

}

func (h *studentHandler) DownloadPdfHandler(w http.ResponseWriter, r *http.Request) {
	pdf, err := h.repo.DownloadPdf(r)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", `attachment; filename="attendance_report.pdf"`)
	w.WriteHeader(http.StatusOK)

	if _, err := pdf.WriteTo(w); err != nil {
		log.Println(err)
	}
}
