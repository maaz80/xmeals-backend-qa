import express from "express";
import bodyParser from "body-parser";
import dotenv from "dotenv";
import cors from "cors";
import paymentRoutes from "./routes/paymentRoutes.js";
import googleMapsRoutes from "./routes/googleMapsRoutes.js";
import walletRoutes from "./routes/walletRoutes.js";
import razorpayWebhookRoute from "./routes/razorpayWebhookRoute.js";
import orderRoutes from './routes/orderRoutes.js'
import whatsappRoutes from './routes/whatsappRoutes.js'
import './controllers/orderController/orderTimeWorker.js'
import { startOrderInsertListener } from "./listener/orderInsertListener.js";
// Disable bodyParser ONLY for Razorpay Webhook
startOrderInsertListener();
dotenv.config();
const app = express();
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
app.use("/api/razorpay", razorpayWebhookRoute);
app.use(bodyParser.json());
app.use("/webhook", orderRoutes);      // /webhook/order-created
app.use("/webhook", whatsappRoutes);   // /webhook/whatsapp
app.use('/api', paymentRoutes);
app.use('/', googleMapsRoutes);
app.use('/api', walletRoutes);

app.listen(process.env.PORT || 4000, () =>
  console.log("Server running on port", process.env.PORT || 3000)
);
