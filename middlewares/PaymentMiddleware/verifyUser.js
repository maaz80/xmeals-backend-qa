import { supabase } from "../../config/supbase.js";

export const verifyUser = async (req, res, next) => {
  const authHeader = req.headers["authorization"];
  if (!authHeader) return res.status(401).json({ error: "No token" });

  const token = authHeader.split(" ")[1];
  const { data, error } = await supabase.auth.getUser(token);

  if (error || !data?.user) {
    return res.status(401).json({ error: "Invalid token" });
  }

  //if user exist and user is not blocked then only place user in req.user and also check if user_id in payload and in here are same
  const { data: userData, error: userError } = await supabase
    .from("user")
    .select("blocked")
    .eq("user_id", data.user.id)
    .single();

    console.log("userData in verify user",userData);
  if (userError || !userData) {
    return res.status(401).json({ error: "User not found or blocked" });
  }

  if (userData.blocked) {
    return res.status(403).json({ error: "User is blocked" });
  }

  if (data?.user?.id === req?.body?.orderPayload?.p_user_id) {
    req.user = data.user;
  }else{
    return res.status(403).json({ error: "User ID mismatch" });
  }
  next();
};
