import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.*;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author xiaorui
 */
@MultipartConfig
@Slf4j
public class Servlet extends HttpServlet {
    private Connection connection;
    private Gson gson = new Gson();
    private static final String GET = "GET";
    private static final String POST = "POST";

    public void init() throws ServletException {
        super.init();
        try {
            connection = AlbumDAO.getConnection();
            log.info("Connected to DB!");
        } catch (SQLException e) {
            log.error("Error establishing database connection.", e);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        res.setContentType("application/json");
        String urlPath = req.getPathInfo();

        // check have a URL
        if (urlPath == null || urlPath.isEmpty()) {
            log.warn("Invalid URL path received in doGet.");
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        // GET: /albums/{albumID}
        String[] urlPaths = urlPath.split("/");
        if (!isUrlValid(urlPaths, "GET")) {
            ErrorMsg errorMsg = new ErrorMsg("Invalid URL path received in doGet.");
            res.getWriter().write(gson.toJson(errorMsg));
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            int albumId = Integer.parseInt(urlPaths[1]);
            AlbumInfo albumInfo = null;
            try {
                albumInfo = AlbumDAO.getAlbum(albumId, connection);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (albumInfo != null) {
                res.setStatus(HttpServletResponse.SC_OK);
                res.getWriter().write(gson.toJson(albumInfo));
            } else {
                ErrorMsg errorMsg = new ErrorMsg("Album not found.");
                res.getWriter().write(gson.toJson(errorMsg));
                res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        res.setContentType("application/json");

        // check if the request has multipart
        if (!req.getContentType().regionMatches(true, 0, "multipart/", 0, 10)) {
            ErrorMsg errorMsg = new ErrorMsg("Received POST request without multipart/form-data enctype.");
            res.getWriter().write(gson.toJson(errorMsg));
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        // check URL format
        String urlPath = req.getPathInfo();
        if (urlPath != null) {
            log.warn("URL format is invalid.");
            return;
        }

        // process multipart request
        try {
            // Handle Profile Part
            AlbumInfo profile = null;
            Part profilePart = req.getPart("profile");
            if (profilePart != null) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(profilePart.getInputStream()))) {
                    String profileJson = reader.lines().collect(Collectors.joining());
                    profile = gson.fromJson(profileJson, AlbumInfo.class);
                }
            }

            // Handle Image Part
            byte[] imageBytes = null;
            Part imagePart = req.getPart("image");
            if (imagePart != null) {
                imageBytes = convertPartToBytes(imagePart);
            }

            // Persist json information and image into database
            Optional<Integer> albumId = AlbumDAO.saveAlbum(profile, imageBytes, connection);

            // calculate the size of the image
            int imageSize = imageBytes == null ? 0 : imageBytes.length;
            String albumIdString = albumId.isPresent() ? String.valueOf(albumId.get()) : null;

            ImageMetaData imageMetaData = new ImageMetaData(albumIdString, String.valueOf(imageSize) + " bytes");
            res.setStatus(HttpServletResponse.SC_OK);
            res.getWriter().write(gson.toJson(imageMetaData));
        } catch (Exception e) {
            log.error("Error processing the post request.", e);
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }

    }

    private boolean isUrlValid(String[] urlPaths, String methodType) {
        if (GET.equalsIgnoreCase(methodType)) {
            // Validating /albums/{albumID}
            if (urlPaths.length != 2) {
                return false;
            }
            try {
                Integer.parseInt(urlPaths[1]);
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    private byte[] convertPartToBytes(Part part) throws IOException {
        try (InputStream inputStream = part.getInputStream()) {
            return inputStream.readAllBytes();
        }
    }
}
