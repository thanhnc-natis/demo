import React, { useEffect, useRef, useState, useCallback } from "react";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

const formatDate = (value) => {
  if (!value) return "-";
  try {
    return new Date(value).toLocaleString("vi-VN", { hour12: false });
  } catch {
    return value;
  }
};

const DocumentManager = ({ onOpenPersonSearch = () => {} }) => {
  const [documents, setDocuments] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [selectedDoc, setSelectedDoc] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [uploading, setUploading] = useState(false);
  const [extractingId, setExtractingId] = useState(null);
  const [downloadingId, setDownloadingId] = useState(null);
  const [kgResult, setKgResult] = useState(null);
  const [imageModalDoc, setImageModalDoc] = useState(null);
  const [docImages, setDocImages] = useState([]);
  const [loadingImages, setLoadingImages] = useState(false);
  const [imageError, setImageError] = useState("");
  const fileInputRef = useRef(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [previewUrl, setPreviewUrl] = useState(null);
  const resetPreviewUrl = useCallback(() => {
    setPreviewUrl((prev) => {
      if (prev && prev.startsWith("blob:")) {
        URL.revokeObjectURL(prev);
      }
      return null;
    });
  }, []);

  const fetchDocuments = async () => {
    try {
      const resp = await fetch(`${API_URL}/documents`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      setDocuments(Array.isArray(data) ? data : []);
    } catch (err) {
      setError(err.message || "Không tải được danh sách tài liệu");
    }
  };

  useEffect(() => {
    fetchDocuments();
  }, []);

  useEffect(() => {
    return () => {
      resetPreviewUrl();
    };
  }, [resetPreviewUrl]);

  useEffect(() => {
    if (!modalOpen) return undefined;
    const onKeyDown = (event) => {
      if (event.key === "Escape") {
        resetPreviewUrl();
        setModalOpen(false);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [modalOpen, resetPreviewUrl]);

  const fetchDetail = async (docId) => {
    setLoading(true);
    setError("");
    resetPreviewUrl();
    try {
      const resp = await fetch(`${API_URL}/documents/${docId}`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      setSelectedDoc(data);
      setSelectedId(docId);
      setKgResult(null);
      setModalOpen(true);
      const blobUrl = await fetchPreviewBlob(docId);
      if (blobUrl) {
        setPreviewUrl(blobUrl);
      }
    } catch (err) {
      setError(err.message || "Không tải được chi tiết tài liệu");
    } finally {
      setLoading(false);
    }
  };

  const handleUpload = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    setUploading(true);
    setError("");
    try {
      const form = new FormData();
      form.append("file", file);
      const resp = await fetch(`${API_URL}/documents/upload`, {
        method: "POST",
        body: form,
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const doc = await resp.json();
      setDocuments((prev) => [doc, ...prev]);
      setSelectedDoc(doc);
      setSelectedId(doc.id);
      setKgResult(null);
    } catch (err) {
      setError(err.message || "Tải lên thất bại");
    } finally {
      setUploading(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  };

  const handleDelete = async (docId) => {
    if (!window.confirm("Xóa tài liệu này?")) return;
    try {
      const resp = await fetch(`${API_URL}/documents/${docId}`, {
        method: "DELETE",
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      setDocuments((prev) => prev.filter((d) => d.id !== docId));
      if (selectedId === docId) {
        setSelectedDoc(null);
        setSelectedId(null);
        setKgResult(null);
        resetPreviewUrl();
        setModalOpen(false);
      }
    } catch (err) {
      setError(err.message || "Không thể xóa tài liệu");
    }
  };

  const handleExtractKg = async (docId) => {
    setExtractingId(docId);
    setError("");
    try {
      const resp = await fetch(`${API_URL}/documents/${docId}/extract-kg`, {
        method: "POST",
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      setKgResult(data);
    } catch (err) {
      setError(err.message || "Không thể trích xuất KG");
    } finally {
      setExtractingId(null);
    }
  };

  const pdfMetadata = selectedDoc?.metadata?.pdf_metadata || {};

  const handleRowKeyDown = (event, docId) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      fetchDetail(docId);
    }
  };

  const handleExtractImages = async (doc) => {
    if (!doc?.id) return;
    setImageModalDoc(doc);
    setDocImages([]);
    setImageError("");
    setLoadingImages(true);
    try {
      const resp = await fetch(`${API_URL}/documents/${doc.id}/images?limit=12`);
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || "Không trích xuất được ảnh");
      setDocImages(data.images || []);
    } catch (err) {
      setImageError(err.message || "Không trích xuất được ảnh");
    } finally {
      setLoadingImages(false);
    }
  };

  const closeImagesModal = () => {
    setImageModalDoc(null);
    setDocImages([]);
    setImageError("");
  };

  const handleSendImageToPersonSearch = (image) => {
    if (!image || typeof window === "undefined" || !onOpenPersonSearch) return;
    try {
      const byteCharacters = window.atob(image.data || "");
      const byteNumbers = new Array(byteCharacters.length);
      for (let i = 0; i < byteCharacters.length; i += 1) {
        byteNumbers[i] = byteCharacters.charCodeAt(i);
      }
      const byteArray = new Uint8Array(byteNumbers);
      const blob = new Blob([byteArray], { type: image.media_type || "image/png" });
      const fileName = `${imageModalDoc?.original_name || "document"}-p${(image.page ?? 0) + 1}.png`;
      const file = new File([blob], fileName, { type: image.media_type || "image/png" });
      const preview = URL.createObjectURL(blob);
      onOpenPersonSearch?.({ file, preview });
      closeImagesModal();
      setModalOpen(false);
    } catch (err) {
      setImageError(err.message || "Không thể gửi ảnh sang Face Search");
    }
  };

  const handleDownload = async (event, doc) => {
    event.stopPropagation();
    setDownloadingId(doc.id);
    setError("");
    try {
      const resp = await fetch(`${API_URL}/documents/${doc.id}/file`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = doc.original_name || doc.filename || `tai-lieu-${doc.id}.pdf`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError(err.message || "Không thể tải xuống tệp");
    } finally {
      setDownloadingId(null);
    }
  };

  const fetchPreviewBlob = async (docId) => {
    try {
      const resp = await fetch(`${API_URL}/documents/${docId}/file`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const blob = await resp.blob();
      return URL.createObjectURL(blob);
    } catch (err) {
      console.error("Preview download failed", err);
      return null;
    }
  };

  return (
    <div className="h-full w-full bg-gray-950 text-white p-4 flex flex-col gap-4 overflow-hidden">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold">OCR Document Manager</h2>
          <p className="text-sm text-gray-400">
            Tải lên PDF, xem metadata, tóm tắt và trích xuất Knowledge Graph.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => fileInputRef.current?.click()}
            className="px-4 py-2 rounded bg-emerald-600 hover:bg-emerald-500 font-semibold text-sm disabled:opacity-60"
            disabled={uploading}
          >
            {uploading ? "Đang tải..." : "Upload PDF"}
          </button>
          <input
            ref={fileInputRef}
            type="file"
            accept="application/pdf"
            className="hidden"
            onChange={handleUpload}
          />
        </div>
      </div>

      {error && <div className="text-red-400 text-sm">{error}</div>}

      <div className="flex-1 overflow-hidden">
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-3 h-full overflow-auto">
          <table className="w-full text-sm">
            <thead className="text-gray-400 uppercase text-xs border-b border-gray-800">
              <tr>
                <th className="text-left py-2">Tên</th>
                <th className="text-left py-2 w-20">Trang</th>
                <th className="text-left py-2">Tóm tắt</th>
                <th className="text-left py-2">Thời gian</th>
                <th className="text-right py-2">Hành động</th>
              </tr>
            </thead>
            <tbody>
              {documents.length === 0 && (
                <tr>
                  <td colSpan={5} className="text-center py-6 text-gray-500">
                    Chưa có tài liệu nào.
                  </td>
                </tr>
              )}
              {documents.map((doc) => (
                <tr
                  key={doc.id}
                  onClick={() => fetchDetail(doc.id)}
                  onKeyDown={(event) => handleRowKeyDown(event, doc.id)}
                  tabIndex={0}
                  role="button"
                  className={`border-b border-gray-850 hover:bg-gray-850 cursor-pointer focus:outline-none focus-visible:ring focus-visible:ring-indigo-500/50 ${
                    selectedId === doc.id ? "bg-gray-850" : ""
                  }`}
                >
                  <td className="py-3 text-left text-indigo-100 font-semibold">
                    {doc.original_name}
                  </td>
                  <td className="py-3">{doc.pages || 0}</td>
                  <td className="py-3 text-gray-300">
                    <span
                      className="block text-sm"
                      style={{
                        display: "-webkit-box",
                        WebkitLineClamp: 2,
                        WebkitBoxOrient: "vertical",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                      }}
                      title={doc.summary || "-"}
                    >
                      {doc.summary || "-"}
                    </span>
                  </td>
                  <td className="py-3 text-gray-400">{formatDate(doc.created_at)}</td>
                  <td className="py-3 text-right">
                    <div className="flex gap-2 justify-end">
                      <button
                        onClick={(event) => {
                          event.stopPropagation();
                          handleExtractImages(doc);
                        }}
                        className="px-3 py-1 bg-slate-700 text-xs rounded hover:bg-slate-600"
                      >
                        Faces
                      </button>
                      <button
                        onClick={(event) => handleDownload(event, doc)}
                        className="px-3 py-1 bg-blue-700 text-xs rounded hover:bg-blue-600 disabled:opacity-50"
                        disabled={downloadingId === doc.id}
                      >
                        {downloadingId === doc.id ? "Dang tai..." : "Tai xuong"}
                      </button>
                      <button
                        onClick={(event) => {
                          event.stopPropagation();
                          handleExtractKg(doc.id);
                        }}
                        className="px-3 py-1 bg-purple-700 text-xs rounded hover:bg-purple-600 disabled:opacity-50"
                        disabled={extractingId === doc.id}
                      >
                        {extractingId === doc.id ? "Đang trích..." : "Trích KG"}
                      </button>
                      <button
                        onClick={(event) => {
                          event.stopPropagation();
                          handleDelete(doc.id);
                        }}
                        className="px-3 py-1 bg-red-700 text-xs rounded hover:bg-red-600"
                      >
                        Xóa
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <p className="text-xs text-gray-500 mt-3">Chon tai lieu de xem chi tiet trong cua so bat len.</p>
        {loading && <div className="text-sm text-gray-400 mt-2">Dang tai chi tiet...</div>}
      </div>

      {modalOpen && selectedDoc && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
          onClick={() => {
            resetPreviewUrl();
            setModalOpen(false);
          }}
        >
          <div
            className="w-full max-w-4xl bg-gray-900 border border-gray-800 rounded-xl shadow-2xl overflow-hidden"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
              <div>
                <h3 className="text-lg font-semibold">{selectedDoc.original_name}</h3>
                <p className="text-xs text-gray-400">
                  {formatDate(selectedDoc.created_at)} • {selectedDoc.pages || 0} trang
                </p>
              </div>
              <button
                onClick={() => {
                  resetPreviewUrl();
                  setModalOpen(false);
                }}
                className="px-3 py-1 text-sm rounded bg-gray-800 hover:bg-gray-700"
              >
                Đóng
              </button>
            </div>

            <div className="p-4 space-y-4 max-h-[80vh] overflow-y-auto">
              {previewUrl && (
                <div>
                  <h4 className="text-sm font-semibold mb-1 text-indigo-300">Xem nhanh</h4>
                  <iframe
                    title="Document preview"
                    src={`${previewUrl}#toolbar=0`}
                    className="w-full h-64 rounded border border-gray-800 bg-gray-950"
                  />
                </div>
              )}

              <div>
                <h4 className="text-sm font-semibold mb-1 text-indigo-300">Tóm tắt</h4>
                <p className="text-sm text-gray-200 whitespace-pre-wrap">
                  {selectedDoc.summary || "Chưa có tóm tắt."}
                </p>
              </div>

              <div>
                <h4 className="text-sm font-semibold mb-1 text-indigo-300">Trích đoạn</h4>
                <p className="text-sm text-gray-300 whitespace-pre-wrap max-h-40 overflow-auto">
                  {selectedDoc.text_excerpt || "Không có nội dung."}
                </p>
              </div>

              <div>
                <h4 className="text-sm font-semibold mb-1 text-indigo-300">Metadata chính</h4>
                <div className="grid grid-cols-2 gap-3 text-xs text-gray-300 bg-gray-950 border border-gray-800 rounded p-3">
                  <div>
                    <div className="text-gray-400 uppercase text-[10px]">Tác giả</div>
                    <div>{pdfMetadata.Author || "-"}</div>
                  </div>
                  <div>
                    <div className="text-gray-400 uppercase text-[10px]">Người tạo</div>
                    <div>{pdfMetadata.Creator || "-"}</div>
                  </div>
                  <div>
                    <div className="text-gray-400 uppercase text-[10px]">Producer</div>
                    <div>{pdfMetadata.Producer || "-"}</div>
                  </div>
                  <div>
                    <div className="text-gray-400 uppercase text-[10px]">Ngày tạo</div>
                    <div>{pdfMetadata.CreationDate || "-"}</div>
                  </div>
                </div>
              </div>

              <div>
                <h4 className="text-sm font-semibold mb-1 text-indigo-300">Metadata</h4>
                <pre className="bg-gray-950 border border-gray-800 rounded p-3 text-xs overflow-auto">
                  {JSON.stringify(selectedDoc.metadata || {}, null, 2)}
                </pre>
              </div>

              {kgResult && (
                <div>
                  <h4 className="text-sm font-semibold mb-1 text-emerald-300">
                    Knowledge Graph (neo4j: {kgResult.neo4j_upserted ? "Đã cập nhật" : "Chưa kết nối"})
                  </h4>
                  <pre className="bg-gray-950 border border-emerald-800/50 rounded p-3 text-xs overflow-auto">
                    {JSON.stringify(kgResult.kg, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {imageModalDoc && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
          <div className="w-full max-w-5xl bg-gray-900 border border-gray-800 rounded-xl shadow-2xl overflow-hidden">
            <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
              <div>
                <h3 className="text-lg font-semibold">
                  Ảnh trích xuất — {imageModalDoc.original_name || imageModalDoc.filename}
                </h3>
                <p className="text-xs text-gray-400">Chọn ảnh phù hợp để gửi sang Face Search.</p>
              </div>
              <button
                onClick={closeImagesModal}
                className="px-3 py-1 text-sm rounded bg-gray-800 hover:bg-gray-700"
              >
                Đóng
              </button>
            </div>
            <div className="p-4 max-h-[80vh] overflow-y-auto space-y-3">
              {loadingImages && <div className="text-sm text-gray-300">Đang trích ảnh...</div>}
              {!loadingImages && imageError && (
                <div className="text-sm text-red-400">{imageError}</div>
              )}
              {!loadingImages && !imageError && docImages.length === 0 && (
                <div className="text-sm text-gray-400">Không tìm thấy ảnh trong tài liệu này.</div>
              )}
              {!loadingImages && docImages.length > 0 && (
                <div className="grid grid-cols-3 gap-4">
                  {docImages.map((img) => (
                    <div
                      key={img.id}
                      className="bg-gray-950 border border-gray-800 rounded-lg p-2 space-y-2 flex flex-col"
                    >
                      <img
                        src={`data:${img.media_type || 'image/png'};base64,${img.data}`}
                        alt={img.id}
                        className="w-full h-40 object-contain rounded border border-gray-800 bg-gray-900"
                      />
                      <div className="text-xs text-gray-400">
                        Trang {(img.page ?? 0) + 1} • {img.media_type?.replace('image/', '') || 'png'}
                      </div>
                      <button
                        onClick={() => handleSendImageToPersonSearch(img)}
                        className="px-3 py-2 text-xs font-semibold rounded bg-indigo-600 hover:bg-indigo-500"
                      >
                        Gửi sang Face Search
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default DocumentManager;
