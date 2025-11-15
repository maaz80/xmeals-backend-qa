import express from "express";
import bodyParser from "body-parser";
import dotenv from "dotenv";
import cors from "cors";
import paymentRoutes from "./routes/paymentRoutes.js";
import googleMapsRoutes from "./routes/googleMapsRoutes.js";
import walletRoutes from "./routes/walletRoutes.js";

dotenv.config();
const app = express();
app.use(bodyParser.json());

const allowedOrigins = process.env.ORIGIN?.split(",").map(origin => origin.trim());

app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error("Not allowed by CORS"));
      }
    },
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
    credentials: true,
  })
);

app.use('/api', paymentRoutes);
app.use('/', googleMapsRoutes);
app.use('/api', walletRoutes);

app.listen(process.env.PORT || 4000, () =>
  console.log("Server running on port", process.env.PORT || 3000)
);
