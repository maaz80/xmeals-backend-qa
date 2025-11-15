// create route for payment
import { Router } from "express";
import { updateWalletBalance } from "../controllers/walletControllers/walletController.js";
import { verifyUserForWallet } from "../middlewares/WalletMiddleware/verifyUserForWallet.js";

const router = Router();

router.post('/wallet/increase',verifyUserForWallet, updateWalletBalance);
router.post('/wallet/decrease',verifyUserForWallet, updateWalletBalance);
export default router;