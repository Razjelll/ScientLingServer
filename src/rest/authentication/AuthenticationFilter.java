package rest.authentication;

import javax.naming.NamingException;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by Razjelll on 27.04.2017.
 */
public class AuthenticationFilter implements javax.servlet.Filter{

    private static final String AUTHENTICATION_HEADER = "Authorization";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        if(servletRequest instanceof HttpServletRequest){
            HttpServletRequest httpServletRequest = (HttpServletRequest)servletRequest;
            String authCredentials = httpServletRequest.getHeader(AUTHENTICATION_HEADER);
            AuthenticationService authenticationService = new AuthenticationService();
            boolean authenticationStatus = false;
            try {
                authenticationStatus = authenticationService.authenticate(authCredentials);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (NamingException e) {
                e.printStackTrace();
            }
            if(authenticationStatus){
                filterChain.doFilter(servletRequest, servletResponse);
            } else {
                if(servletResponse instanceof HttpServletResponse){
                    HttpServletResponse httpServletResponse = (HttpServletResponse)servletResponse;
                    httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                }
            }

        }
    }

    @Override
    public void destroy() {

    }
}
